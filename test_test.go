// coding is a world

package lamportClock

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// func TestList(t *testing.T) {
// 	arr := list.New()
// 	arr.PushBack(10)
// 	arr.PushBack(9)
// 	arr.PushBack(8)

// 	fmt.Printf("the head is %d\n", arr.Front().Value)
// 	node := arr.Front()

// 	for node != nil {
// 		fmt.Printf("the element value is : %d\n", node.Value)
// 		node = node.Next()
// 	}

// }

// first test basic function of mutulaLock

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "muLock-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func cleanup(mutualLocks []*MutualLock) {
	for i := 0; i < len(mutualLocks); i++ {
		if mutualLocks[i] != nil {
			mutualLocks[i].Kill()
		}
	}
}

//
// test basic function of mutualLock
func TestBasic(t *testing.T) {
	fmt.Println("------------testing Basic----------")
	runtime.GOMAXPROCS(4) // also set 4.

	nservers := 3
	var mutualLocks []*MutualLock = make([]*MutualLock, nservers)
	var mutualLockNames []string = make([]string, nservers)
	defer cleanup(mutualLocks)

	for i := 0; i < nservers; i++ {
		mutualLockNames[i] = port("basic", i)
	}
	for i := 0; i < nservers; i++ {
		mutualLocks[i] = MakeAMutualLock(mutualLockNames, i)
	}

	mutualLocks[0].LockResource()

	go func() {
		time.Sleep(time.Second * 1)
		mutualLocks[0].UnLockResource()
	}()

	mutualLocks[1].LockResource()
	mutualLocks[1].UnLockResource()

	fmt.Println("..... passed ......")

}

func TestMultiInitialLock(t *testing.T) {
	fmt.Println("------------testing MultiInitialLock----------")
	runtime.GOMAXPROCS(4) // also set 4.
	nservers := 3
	var mutualLocks []*MutualLock = make([]*MutualLock, nservers)
	var mutualLockNames []string = make([]string, nservers)
	defer cleanup(mutualLocks)

	for i := 0; i < nservers; i++ {
		mutualLockNames[i] = port("multiInitial", i)
	}

	for i := 0; i < nservers; i++ {
		mutualLocks[i] = MakeAMutualLock(mutualLockNames, i)
	}

	// for i := 0; i < nservers; i++ {
	// 	go func() {
	// 		mutualLocks[i].LockResource()
	// 	}()
	// }

	mutualLocks[0].LockResource()
	mutualLocks[0].UnLockResource()

	go func() {
		mutualLocks[0].LockResource()
	}()

	go func() {
		mutualLocks[1].LockResource()
	}()

	go func() {
		mutualLocks[2].LockResource()
	}()

	time.Sleep(time.Second * 10)

	fmt.Println("..... passed ......")

}
