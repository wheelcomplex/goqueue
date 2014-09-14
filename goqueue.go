/*
 Package golfstk provides util functions for general programing
*/

package goqueue

//import "fmt"

//import "time"
import "runtime"

// FILO/FIFO stack with max item limit and discard support
type Stack struct {
	in      chan interface{}      //
	out     chan interface{}      //
	max     int64                 // max item in stack(chan+pool)
	ptr     int64                 // current position
	discard bool                  //
	pool    map[int64]interface{} // pool
	exited  chan struct{}         // flag channel, channel closed after stack closed
}

// NewStack return new Stack and lauch stack manager goroutine
// chanBufferSize for input/output channel buffer size, for forward speed recommand buffer size >= 256
// poolSize for stack pool size
// discard control how to do when stack is full, true for drop new item, false for waiting
// max item stay in stack will be input channel buffer(chanBufferSize) + output channel buffer(chanBufferSize) + poolSize
// set poolSize <= 0 turn stack into FIFO pipe
// set poolSize < 0 for unlimited stack
// input channel closed when stack.Close()
func NewStack(chanBufferSize int64, poolSize int64, discard bool) *Stack {
	var self *Stack
	if chanBufferSize < 0 {
		chanBufferSize = 0
	}
	self = &Stack{
		max:     poolSize,
		ptr:     0,
		discard: discard,
		in:      make(chan interface{}, chanBufferSize),
		out:     make(chan interface{}, chanBufferSize),
		pool:    make(map[int64]interface{}),
		exited:  make(chan struct{}, 0),
	}
	// slice index from 1, so we will fill index 0 with
	self.pool[0] = nil
	switch {
	case self.max == 0:
		// FIFO pipe
		go self.pipe()
	default:
		// FILO
		go self.manager()
	}
	return self
}

// pipe forward input item to output in FIFO mode
// if output blocked and self.discard == true, new input item discarded, otherwise blocked at output
func (self *Stack) pipe() {
	// fast forward
	var in interface{}
	if self.discard {
		for in = range self.in {
			select {
			case self.out <- in:
			default:
				// discarded
			}
		}
	} else {
		for in = range self.in {
			self.out <- in
		}
	}
	close(self.out)
}

// manager forward input item to output
// save item to pool when output blocked
// if pool size reach max(then mean output blocked),
// and self.discard == true, new input item discarded,
// otherwise blocked at output
func (self *Stack) manager() {
	var in interface{} = nil
	var ok bool
	var loop bool = true
	for {
		// fast forward
		for in = range self.in {
			select {
			case self.out <- in:
				continue
			default:
				self.poolIn(&in)
				// out is blocked
				loop = false
			}
			if loop == false {
				break
			}
		}
		// in closed or out blocked
		// slow forwad
		select {
		case in, ok = <-self.in:
			if !ok {
				// in closed
				//fmt.Printf("fast forward, stack is closed when pool size %d and got in: %v\n", self.ptr, in)
				return
			}
			select {
			case self.out <- in:
				// goto fast forward
				loop = true
				continue
			default:
				self.poolIn(&in)
			}
		default:
		}
		if self.ptr == 0 {
			// loop back to fast forward
			runtime.Gosched()
			continue
		}
		// try to flush out when input blocked
		select {
		case self.out <- self.pool[self.ptr]:
			// pop out
			// -- will slow down
			self.ptr--
			//fmt.Println("flush out, stack size", self.ptr)
		default:
		}
	}
	return
}

//
func (self *Stack) poolIn(ptr *interface{}) {
	if self.ptr < self.max || self.max < 0 {
		// push in
		// ++ will slow down
		self.ptr++
		self.pool[self.ptr] = *ptr
	} else if self.discard {
		// just discard
		//fmt.Println("stack block for full, discarded, max size", self.max, "current", self.ptr)
	} else {
		// already full
		//fmt.Println("stack block for full, max size", self.max, "current", self.ptr)
		// blocked if Out is blocked, and we do not read input for pool is full
		// do not try to read more in
		self.out <- *ptr
	}
	return
}

//
func (self *Stack) flushExit() {
	// blocking flush in buffer
	//fmt.Println("befor in buffer flush, pool size", self.ptr, "in buffer len", len(self.in))
	var in interface{}
	for in = range self.in {
		if self.ptr < self.max || self.max < 0 {
			self.ptr++
			self.pool[self.ptr] = in
			//fmt.Println("flush in, pool size", self.ptr, "in buffer len", len(self.in))
		} else {
			if self.discard {
				//fmt.Println("out channel blocked, and pool reach max size, discard", self.ptr)
			} else {
				//fmt.Println("out channel blocked, and pool reach max size, blocking", self.ptr)
				self.out <- in
			}
		}
	}
	//fmt.Println("befor out buffer flush, pool size", self.ptr, "out buffer len", len(self.out))
	// flush pool to out
	for self.ptr > 0 {
		self.out <- self.pool[self.ptr]
		// pop out
		self.ptr--
		//fmt.Println("out buffer flush, pool size", self.ptr, "out buffer len", len(self.out))
	}
	//fmt.Println("after out buffer flush, pool size", self.ptr, "out buffer len", len(self.out))
	close(self.out)
	// release memory
	for key, _ := range self.pool {
		delete(self.pool, key)
	}
	self.pool = nil
	return
}

// Close stop stack manager and release pool
// write to a Closed self.In() will rise panic("write at close channel")
// user should handle panic by recover()
func (self *Stack) Close() {
	select {
	case <-self.exited:
		// already closed
		return
	default:
	}
	close(self.exited)
	close(self.in)
	if self.max != 0 {
		go self.flushExit()
	}
	return
}

// ClosedChan return a flag channel for stack closed test
// channel will closed after self.Close()
// user should use: select-read block to test channel status
func (self *Stack) ClosedChan() <-chan struct{} {
	return self.exited
}

// IsClosed return true if stack closed
func (self *Stack) IsClosed() bool {
	select {
	case <-self.exited:
		return true
	default:
	}
	return false
}

// push in into stack, return nil for ok, if stack already closed, return error
func (self *Stack) Push(in interface{}) error {
	defer func() error {
		//err := recover()
		//if err != nil {
		//	fmt.Println("defer-recover:", err)
		//}
		//return err.(error)
		return recover().(error)
	}()
	self.in <- in
	return nil
}

// In return channel for stack push
// channel will closed after self.Close()
// user have to handle panic("write to closed channel") if try to write after Close()
// or use self.Push(in), which have a defer-recover panic handle
func (self *Stack) In() chan<- interface{} {
	return self.in
}

// Out return channel for stack pop
// channel will closed after self.Close()
// read from a closed channel will got <nil>
func (self *Stack) Out() <-chan interface{} {
	return self.out
}

// Lock Free Stack without channel

/*
package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	MAX_DATA_SIZE = 10000
)

// lock free lfstk
type LFstack struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

// one node in lfstk
type node struct {
	val  interface{}
	next unsafe.Pointer
}

// CompareAndSwapPointer executes the compare-and-swap operation for a unsafe.Pointer value.
// func CompareAndSwapPointer(addr *unsafe.Pointer, old, new unsafe.Pointer) (swapped bool)

// lfstk functions
func (self *LFstack) Push(val interface{}) {
	newValue := unsafe.Pointer(&node{val: val, next: nil})
	var tail, next unsafe.Pointer
	for {
		tail = self.tail
		next = ((*node)(tail)).next
		if next != nil {
			atomic.CompareAndSwapPointer(&(self.tail), tail, next)
		} else if atomic.CompareAndSwapPointer(&((*node)(tail).next), nil, newValue) {
			break
		}
		runtime.Gosched()
	}
}

func (self *LFstack) Pop() (val interface{}, success bool) {
	var head, tail, next unsafe.Pointer
	for {
		head = self.head
		tail = self.tail
		next = ((*node)(head)).next
		if head == tail {
			if next == nil {
				return nil, false
			} else {
				atomic.CompareAndSwapPointer(&(self.tail), tail, next)
			}
		} else {
			val = ((*node)(next)).val
			if atomic.CompareAndSwapPointer(&(self.head), head, next) {
				return val, true
			}
		}
		runtime.Gosched()
	}
	return
}

func qin(start int) {
	defer wg.Done()
	start = start * 1000
	//fmt.Println("start = ", start)
	for j := 0; j < MAX_DATA_SIZE; j++ {
		start++
		fmt.Println("enq = ", start)
		lfstk.Push(start)
	}
}

func qout() {
	ok := false
	var val interface{}
	defer wg.Done()
	for j := 0; j < MAX_DATA_SIZE; j++ {
		val, ok = lfstk.Pop()
		for !ok {
			val, ok = lfstk.Pop()
			runtime.Gosched()
		}
		fmt.Println("deq = ", val)
	}
}

var wg sync.WaitGroup

var lfstk *LFstack

func main() {

	lfstk = new(LFstack)
	lfstk.head = unsafe.Pointer(new(node))
	lfstk.tail = lfstk.head

	// 9 qin * 10000 * 2 = 1800000
	for i := 1; i < 10; i++ {
		wg.Add(1)
		go qin(i)
	}

	for i := 0; i < 9; i++ {
		wg.Add(1)
		go qout()
	}

	wg.Wait()
}

*/

// lock Free Stack with channel
//