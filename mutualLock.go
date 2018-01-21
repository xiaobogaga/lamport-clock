// " Package lamportClock ..."

package lamportClock

// here we define a distributed mutual lock.
// since we should better not change the code of pvpaxos.
// so here we decide to implement a distributed sequencer

// this distributed sequencer implementation is based on Paper:
// lamport 1978 time, clock...
// in his paper, lamport proposed a distributed mutual lock algorithm by
// using logical clock , here , we implement that.

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// A MutualLock
type MutualLock struct {
	resourceObtained bool // resourceObtained
	requestQueue     *list.List
	time             int
	dead             bool
	l                net.Listener
	me               int
	mutualLocks      []string
	timeLock         sync.Mutex
	requestQueueLock sync.Mutex
	sleepTime        time.Duration
	resource         interface{}
}

// requestMsg
type RequestMsg struct {
	Process int
	Time    int
	AllAck  bool
}

// acknowledgeMsg
type AcknowledgeMsg struct {
	Process int
	Time    int
}

// requestResourceArg
type RequestResourceArg struct {
	Request RequestMsg
}

// RequestResourceReply
type RequestResourceReply struct {
	Acknowledge AcknowledgeMsg
	OK          bool
}

// ReleaseResourceArg
type ReleaseResourceArg struct {
	Time    int
	Process int
}

// ReleaseResourceReply
type ReleaseResourceReply struct {
}

//
// a client can call this method to request lock resource.
// this method will block until obtain the resource.
func (mutualLock *MutualLock) LockResource() error {

	if mutualLock.resourceObtained {

		mutualLock.timeLock.Lock()
		log.Printf("MutualLock[%d] : a lockResource method called but lock is already obtained , time is : %d",
			mutualLock.me, mutualLock.time)
		mutualLock.timeLock.Unlock()

		return nil
	} else {
		// if not obtain, must send to every other mutualLock Server a
		// requestLock request.
		requestMsg := RequestMsg{}
		requestMsg.Process = mutualLock.me
		requestMsg.AllAck = false

		// maybe deadlock
		mutualLock.timeLock.Lock()
		mutualLock.time++ // need push forward the clock by 1
		requestMsg.Time = mutualLock.time
		log.Printf("MutualLock[%d] : a lockResource method called and will broadcast , time is : %d",
			mutualLock.me, mutualLock.time)
		mutualLock.timeLock.Unlock()

		mutualLock.requestQueueLock.Lock()
		requestELe := mutualLock.requestQueue.PushBack(requestMsg)
		mutualLock.requestQueueLock.Unlock()

		requestResourceArg := RequestResourceArg{}
		requestResourceArg.Request = requestMsg
		sendAllFinish := true
		needRetry := true

		for needRetry {

			for i, muLock := range mutualLock.mutualLocks {
				requestResourceReply := RequestResourceReply{}
				if i == mutualLock.me {
					// ignore myself
				} else {
					// we should send a request msg to the lock server
					ok := call(muLock, "MutualLock.RequestResource", &requestResourceArg,
						&requestResourceReply)
					if ok == false {
						log.Printf("MutualLock[%d] : call requestResource to mutualLock[%d] failed , time is : %d",
							mutualLock.me, i, requestResourceArg.Request.Time)
						sendAllFinish = false
					} else {
						// call finish
					}
				}
			}
			// if send unfinish , we need retry
			if sendAllFinish {
				needRetry = false
			} else {
				needRetry = true
				log.Printf("MutualLock[%d] : call requestResource to all mutualLock failed , sleep then retry, time is : %d",
					mutualLock.me, requestResourceArg.Request.Time)
				time.Sleep(mutualLock.sleepTime)
			}

		}

		log.Printf("MutualLock[%d] : call requestResource to all mutualLock finished , will wait obtain , time is : %d",
			mutualLock.me, requestResourceArg.Request.Time)

		// after send finish, we can check whether we can obtain the resource.
		// since received all acknowledge , we just need to promise that the
		// request is the head of the request queue.
		resourceObtained := false

		for !resourceObtained {

			mutualLock.requestQueueLock.Lock()
			isMinimal := true
			node := mutualLock.requestQueue.Front()
			for node != nil {
				if node.Value.(RequestMsg).Time < requestELe.Value.(RequestMsg).Time ||
					(node.Value.(RequestMsg).Time == requestELe.Value.(RequestMsg).Time &&
						node.Value.(RequestMsg).Process < requestMsg.Process) {
					isMinimal = false
					break
				}
				node = node.Next()
			}
			mutualLock.requestQueueLock.Unlock()

			if !isMinimal {
				log.Printf("MutualLock[%d] : still doesn't get the resource, will sleep then retry.",
					mutualLock.me)
				time.Sleep(mutualLock.sleepTime) // give others time.
			} else {
				resourceObtained = true
			}

		}

		log.Printf("MutualLock[%d] : get resource success", mutualLock.me)
		mutualLock.resourceObtained = true

	}

	return nil
}

// a client can call unLockResource to unlock the resource.
func (mutualLock *MutualLock) UnLockResource() error {
	// when unlock the resource , just remove the lock request and send release resource request.
	// since it must be the head of the request queue

	if !mutualLock.resourceObtained {
		log.Printf("MutualLock[%d] : unlock the resource, but doesn't obtain the resource.",
			mutualLock.me)
		return nil
	}

	mutualLock.requestQueueLock.Lock()
	mutualLock.requestQueue.Remove(mutualLock.requestQueue.Front())
	mutualLock.requestQueueLock.Unlock()

	// then send the release request
	releaseResourceArg := ReleaseResourceArg{}

	releaseResourceArg.Process = mutualLock.me

	mutualLock.timeLock.Lock()
	mutualLock.time++
	releaseResourceArg.Time = mutualLock.time
	log.Printf("MutualLock[%d] : unlock resource, will broadcast , time is %d .",
		mutualLock.me, mutualLock.time)
	mutualLock.timeLock.Unlock()

	sendAllFinish := true
	needRetry := true
	for needRetry {
		for i, _ := range mutualLock.mutualLocks {
			releaseResourceReply := ReleaseResourceReply{}
			if i == mutualLock.me {
				// ignore itself
			} else {
				ok := call(mutualLock.mutualLocks[i], "MutualLock.ReleaseResource",
					&releaseResourceArg, &releaseResourceReply)
				if ok == false {
					// if false, then the release resource method is failed.
					sendAllFinish = false
				} else {
					// send success
				}
			}
		}

		if sendAllFinish {
			log.Printf("MutualLock[%d] : broadcast release resource success.", mutualLock.me)
			needRetry = false
		} else {
			needRetry = true
			log.Printf("MutualLock[%d] : broadcast release resource failed. will retry .", mutualLock.me)
			time.Sleep(mutualLock.sleepTime)
		}
	}

	// finish
	log.Printf("MutualLock[%d] : unlock success.", mutualLock.me)

	mutualLock.resourceObtained = false
	return nil
}

//
// this method only be called by other mutual lock service.
// see Lamport paper.
func (mutualLock *MutualLock) RequestResource(requestResourceArg *RequestResourceArg,
	requestResourceReply *RequestResourceReply) error {

	// when he received the request , just add it
	// to the requestQueue.

	time := requestResourceArg.Request.Time

	mutualLock.timeLock.Lock()

	if mutualLock.time > time {
		// do nothing
		mutualLock.time++ // push forward the clock by 1
	} else {
		// otherwise , we need to change clock
		mutualLock.time = time + 1
	}

	mutualLock.timeLock.Unlock()

	mutualLock.requestQueueLock.Lock()
	// add the request to the queue
	mutualLock.requestQueue.PushBack(requestResourceArg.Request)

	mutualLock.requestQueueLock.Unlock()

	requestResourceReply.Acknowledge = AcknowledgeMsg{}
	requestResourceReply.Acknowledge.Process = mutualLock.me

	mutualLock.timeLock.Lock()
	requestResourceReply.Acknowledge.Time = mutualLock.time
	log.Printf("MutualLock[%d] : request resource msg from MutualLock[%d], time is : %d",
		mutualLock.me, requestResourceArg.Request.Process, mutualLock.time)
	mutualLock.timeLock.Unlock()

	return nil

}

//
// this method is called by other mutual lock service when they release their resource.
func (mutualLock *MutualLock) ReleaseResource(releaseRequestArg *ReleaseResourceArg,
	reply *ReleaseResourceReply) error {
	// remove all request which from the releaseResourceArg.process

	mutualLock.requestQueueLock.Lock()
	defer mutualLock.requestQueueLock.Unlock()

	size := mutualLock.requestQueue.Len()
	node := mutualLock.requestQueue.Front()
	var temp *list.Element
	var requestMsg RequestMsg
	for i := 0; i < size; i++ {
		requestMsg = node.Value.(RequestMsg)
		if requestMsg.Process == releaseRequestArg.Process &&
			requestMsg.Time <= releaseRequestArg.Time {
			// remove this node.
			temp = node
			node = node.Next()
			mutualLock.requestQueue.Remove(temp)
		}
	}

	log.Printf("MutualLock[%d] : release resource msg from MutualLock[%d], time is : %d",
		mutualLock.me, releaseRequestArg.Process, releaseRequestArg.Time)

	return nil
}

func (mutualLock *MutualLock) Kill() {
	log.Printf("MutualLock[%d] : I will shutdown", mutualLock.me)
	mutualLock.dead = true
}

func MakeAMutualLock(servers []string, me int) *MutualLock {

	mutualLock := new(MutualLock)
	mutualLock.time = 0
	mutualLock.requestQueue = list.New()
	mutualLock.dead = false
	mutualLock.me = me
	mutualLock.mutualLocks = servers
	mutualLock.sleepTime = time.Millisecond * 100

	// make rpc
	rpcs := rpc.NewServer()
	rpcs.Register(mutualLock)
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatalf("MutualLock : net.listen error")
		return nil
	} else {
		mutualLock.l = l
		go func() {
			for mutualLock.dead == false {
				conn, err := mutualLock.l.Accept()
				if err == nil && mutualLock.dead == false {
					go rpcs.ServeConn(conn)
				} else if err == nil {
					conn.Close()
				}
				if err != nil && mutualLock.dead == false {
					fmt.Printf("MutualLock[%v] accept: %v\n", me, err.Error())
					mutualLock.Kill()
				}
			}
		}()
	}

	return mutualLock

}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
