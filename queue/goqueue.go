/*
 Package goqueue provides queue/stack with max pool size and discard for general programing
*/

/*
   demo QPS:
   // sigle cpu, zero channel buffer
   215: -t 30 -c 1 -C 4 -P 4 -B 0 -S 0 -D (discard) VS 256(run2, pipe)
   217: -t 30 -c 1 -C 4 -P 4 -B 0 -S 0 VS 249(run2, pipe)
   155: -t 30 -c 1 -C 1 -P 1 -B 0 -S 0 -D (discard) VS 252(run2, pipe)
   197: -t 30 -c 1 -C 1 -P 1 -B 0 -S 0 VS 250(run2, pipe)
   568: -t 30 -c 1 -C 1 -P 1 -B 1024 -S 0 -D (discard) VS 567(run2, pipe)
   572: -t 30 -c 1 -C 1 -P 1 -B 1024 -S 0 VS 564
   557: -t 30 -c 1 -C 1 -P 1 -B 1024 -S 10240 -D (discard) VS 566(run2, pipe)
   565: -t 30 -c 1 -C 1 -P 1 -B 1024 -S 10240 VS 573
   560: -t 30 -c 1 -C 8 -P 8 -B 1024 -S 10240 -D (discard) VS 571(run2, pipe)
   559: -t 30 -c 1 -C 8 -P 8 -B 1024 -S 10240 VS 570

   // multi cpu
   89: -t 30 -c 16 -C 4 -P 4 -B 0 -S 0 -D (discard) VS 56(run2, pipe)
   87: -t 30 -c 16 -C 4 -P 4 -B 0 -S 0 VS 55(run2, pipe)
   57 : -t 30 -c 16 -C 1 -P 1 -B 0 -S 0 -D (discard) VS 69(run2, pipe)
   61: -t 30 -c 16 -C 1 -P 1 -B 0 -S 0 VS 105(run2, pipe)
   300: -t 30 -c 16 -C 1 -P 1 -B 1024 -S 0 -D (discard) VS 300(run2, pipe)
   296: -t 30 -c 16 -C 1 -P 1 -B 1024 -S 0 VS 298
   301: -t 30 -c 16 -C 1 -P 1 -B 1024 -S 10240 -D (discard) VS 300(run2, pipe)
   301: -t 30 -c 16 -C 1 -P 1 -B 1024 -S 10240 VS 300
   115: -t 30 -c 16 -C 8 -P 8 -B 1024 -S 10240 -D (discard) VS 300(run2, pipe)
   117: -t 30 -c 16 -C 8 -P 8 -B 1024 -S 10240 VS 300

*/

package goqueue

import "time"
import "sync"

// FILO/FIFO stack with max item limit and discard support
type Stack struct {
	in      chan interface{}      //
	out     chan interface{}      //
	max     int64                 // max item in stack(chan+pool)
	ptr     int64                 // current position
	discard bool                  //
	pool    map[int64]interface{} // pool
	closing chan struct{}         // flag channel, notify when stack is full and discard == false
	dead    chan struct{}         // flag channel, channel closed after stack closed
}

// NewStack return new Stack and lauch stack manager goroutine
// chanBufferSize for input/output channel buffer size, for forward speed recommand buffer size >= 256
// poolSize for stack pool size
// discard control how to do when stack is full, true for drop new item, false for waiting
// max item stay in stack will be input channel buffer(chanBufferSize) + output channel buffer(chanBufferSize) + poolSize
// set poolSize <= 0 turn stack into FIFO pipe, this is a in-storage-out forwarder
// set poolSize < 0 for unlimited stack
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
		closing: make(chan struct{}, 0),
		dead:    make(chan struct{}, 0),
	}
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

// Close stop stack manager and release pool
// NOTE: call Stack.Close() func will close input channel
// write to a Closed self.In() will rise panic("send on closed channel"), user should handle panic by recover()
func (self *Stack) Close() {
	//
	// MAKE SURE: user/Close() close self.in -> manager close self.dead  -> flushExit() close self.out
	//
	select {
	case <-self.dead:
		// already closed
		return
	default:
	}
	select {
	case <-self.closing:
		// already closed
	default:
		close(self.closing)
	}
	// NOTE: do not close(self.dead), it wil close by manager
	// NOTE: do not close(self.out), it wil close by flushExit
	// user have no way to close self.in because it is a read only channel
	defer func() {
		// user may be just close self.In()
		recover()
	}()
	close(self.in)
	return
}

// pipe forward input item to output in FIFO mode, for self.max == 0
// if output blocked and self.discard == true, new input item discarded, otherwise blocked at output
func (self *Stack) pipe() {
	//println("enter pipe", self.discard)
	// self.max == 0
	defer func() {
		// self.in already closed by user
		close(self.dead)
		select {
		case <-self.closing:
			// already closed
		default:
			close(self.closing)
		}
		go self.flushExit()
	}()
	// fast forward
	var in interface{}
	if self.discard {
		for in = range self.in {
			// blocking read
			select {
			case self.out <- in:
			default:
				// discarded
			}
		}
		// self.in closed
	} else {
		for in = range self.in {
			select {
			case self.out <- in:
			default:
				select {
				case self.out <- in:
				case <-self.closing:
					// save blocking item
					self.ptr++
					self.pool[self.ptr] = in
					// self.ptr == 1
					return
				}
			}
		}
		// self.in closed
	}
	return
}

// manager forward input item to output, for self.max > 0
// save item to pool when output blocked
// if pool size reach max(then mean output blocked),
// and self.discard == true, new input item discarded,
// otherwise blocked at output
func (self *Stack) manager() {
	var in interface{} = nil
	var ok bool
	defer func() {
		close(self.dead)
		select {
		case <-self.closing:
			// already closed
		default:
			close(self.closing)
		}
		go self.flushExit()
	}()
	//// 5 seconds clean
	cleanTk := time.NewTicker(1e9)
	defer cleanTk.Stop()
	var lastptr int64
	//
	// MAKE SURE: user/Close() close self.in -> manager close self.dead  -> flushExit() close self.out
	//
	// main loop
	//var espstart time.Time
	for {
		// try to compact pool
		// save memory but slow down long running stack ?
		if lastptr > self.ptr {
			for ptr := self.ptr + 1; ptr <= lastptr; ptr++ {
				delete(self.pool, ptr)
			}
			//println("befor FF, pool size", self.ptr, "last", lastptr, "cleaned", lastptr-self.ptr)
			if self.ptr == 0 {
				self.pool = make(map[int64]interface{})
			}
			lastptr = self.ptr
		} else if self.ptr > lastptr {
			lastptr = self.ptr
		}
		if self.ptr == 0 {
			// fast forward, pipe mode
			//println("self.ptr == 0, enter fast forward, pool size", self.ptr, "last", lastptr)
			//espstart = time.Now()
			func() {
				for in = range self.in {
					select {
					case self.out <- in:
						// goto in, fast forward
					default:
						// push in
						// ++ will slow down
						self.ptr++
						self.pool[self.ptr] = in
						// self.ptr == 1
						return
					}
				}
			}()
			// self.in closed or self.ptr == 1
			//println("self.ptr == ", self.ptr, ", exit  fast forward, pool size", self.ptr, "last", lastptr)
			//println("esp", time.Now().Sub(espstart), "self.ptr == ", self.ptr, ", exit fast forward, pool size", self.ptr, "last", lastptr)
		}
		// end of fast forward, self.in closed or self.ptr == 1
		//if self.ptr == 0 {
		//	// stack closed
		//	return
		//}
		//espstart = time.Now()
		// try to compact pool
		// save memory but slow down long running stack ?
		// slow forwad
		for {
			// check input
			select {
			case in, ok = <-self.in:
				// slow by ok check
				if ok == false {
					//println("esp", time.Now().Sub(espstart), "slow forward, stack closed, pool size", self.ptr, "last", lastptr)
					return
				}
				select {
				case self.out <- in:
					// goto in, slow forward
					continue
				default:
					// out is blocked
					// slow by pool size check
					if self.ptr < self.max || self.max < 0 {
						// push in
						// ++ will slow down
						self.ptr++
						self.pool[self.ptr] = in
					} else if self.discard {
						// just discard
						//println("esp", time.Now().Sub(espstart), "slow forward, input discard when pool full, pool size", self.ptr, "last", lastptr)
					} else {
						// already full
						// do not try to read more in
						//println("esp", time.Now().Sub(espstart), "slow forward, output blocked when pool full, pool size", self.ptr, "last", lastptr)
						select {
						case self.out <- in:
						case <-self.closing:
							self.ptr++
							self.pool[self.ptr] = in
							// overflow max, self.ptr == self.max + 1
							//println("esp", time.Now().Sub(espstart), "slow forward, stack closed when pool full, pool size", self.ptr, "last", lastptr)
							return
						}
						//println("esp", time.Now().Sub(espstart), "slow forward, output un-blocked when pool full, pool size", self.ptr, "last", lastptr)
					}
				}
			default:
				// input blocked, output unknow
				if self.ptr > 0 {
					select {
					case self.out <- self.pool[self.ptr]:
						// pop out
						// -- will slow down
						self.ptr--
						// goto input, will not compact pool for busy stack
						// should we check self.ptr == 0 and goto fast forward ?
						continue
					default:
						// input blocked, output blocked
						// goto pool compact
					}
				}
				select {
				case <-cleanTk.C:
					// try to compact pool
					// save memory but slow down long running stack ?
					if lastptr > self.ptr {
						for ptr := self.ptr + 1; ptr <= lastptr; ptr++ {
							delete(self.pool, ptr)
						}
						//println("slow forward, pool size", self.ptr, "last", lastptr, "cleaned", lastptr-self.ptr)
						if self.ptr == 0 {
							self.pool = make(map[int64]interface{})
						}
						lastptr = self.ptr
					} else if self.ptr > lastptr {
						lastptr = self.ptr
					}
				default:
				}
				// all blocked
				if self.ptr > 0 {
					// all blocked, 2-way blocking select
					//println("2-way blocking select")
					select {
					case in, ok = <-self.in:
						// slow by ok check
						if ok == false {
							//println("esp", time.Now().Sub(espstart), "2-way blocking select, stack closed, pool size", self.ptr, "last", lastptr)
							return
						}
						// output already blocked, so push into pool
						if self.ptr < self.max || self.max < 0 {
							// push in
							// ++ will slow down
							self.ptr++
							self.pool[self.ptr] = in
						} else if self.discard {
							// just discard
						} else {
							// already full
							// do not try to read more in
							select {
							case self.out <- in:
							case <-self.closing:
								self.ptr++
								self.pool[self.ptr] = in
								// overflow max, self.ptr == self.max + 1
								//println("esp", time.Now().Sub(espstart), "2-way blocking select, stack closed when pool full, pool size", self.ptr, "last", lastptr)
								return
							}
						}
					case self.out <- self.pool[self.ptr]:
						// pop out
						// -- will slow down
						self.ptr--
					}
				} else {
					// self.ptr == 0, goto fast forward
					break
				}
				// end of all blocked
			}
			// end of check input(select in)
			if self.ptr == 0 {
				// self.ptr == 0, goto fast forward
				//println("esp", time.Now().Sub(espstart), "slow forward, self.ptr == 0, goto fast forward, pool size", self.ptr, "last", lastptr)
				break
			}
		}
		// end of slow forward(for)
	}
	// end of main loop(for)
}

// if user do not stop push in, stack will become pipe channel in flushExit
//
func (self *Stack) flushExit() {
	// blocking flush in buffer
	// self.in already closed
	var in interface{}
	func() {
		// self.in already closed, read buffered item in self.in
		for in = range self.in {
			select {
			case self.out <- in:
				// fast forward
				continue
			default:
				if self.ptr < self.max || self.max < 0 {
					self.ptr++
					self.pool[self.ptr] = in
				} else {
					if self.discard {
						// discarded
					} else {
						// blocked write
						self.out <- in
					}
				}
			}
		}
	}()
	// flush pool to out
	for self.ptr > 0 {
		// blocked write
		self.out <- self.pool[self.ptr]
		// pop out
		self.ptr--
	}
	close(self.out)
	// release memory
	for key, _ := range self.pool {
		delete(self.pool, key)
	}
	self.pool = nil
	return
}

// ClosedChan return a flag channel for stack closed test
// channel will closed after self.Close()
// user should use: select-read block to test channel status
func (self *Stack) ClosedChan() <-chan struct{} {
	return self.dead
}

// IsClosed return true if stack closed
func (self *Stack) IsClosed() bool {
	select {
	case <-self.dead:
		return true
	default:
	}
	return false
}

// SetPoolSize return old PoolSize
// and set pool size to size
func (self *Stack) SetPoolSize(size int64) int64 {
	old := self.max
	self.max = size
	return old
}

const (
	PUSH_OK int = iota
	PUSH_FULL
	PUSH_CLOSED
)

// push in into stack, return PUSH_OK for ok
// return PUSH_FULL when stack is full and blocking == false
// return PUSH_CLOSED when stack is closed
func (self *Stack) Push(in interface{}, blocking bool) int {
	defer func() int {
		if recover() != nil {
			return PUSH_CLOSED
		}
		return PUSH_OK
	}()
	select {
	case <-self.dead:
		return PUSH_CLOSED
	default:
	}
	if blocking {
		self.in <- in
	} else {
		select {
		case self.in <- in:
		default:
			return PUSH_FULL
		}
	}
	return PUSH_OK
}

// In return channel for stack push
// channel will closed after self.Close()
// user have to handle panic("send on closed channel") if try to write after Close()
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

// GetCacheSize return items number in stack(include channel buffered)
func (self *Stack) GetCacheSize() int64 {
	return self.ptr + int64(len(self.in)) + int64(len(self.out))
}

// utils of Stack

// GroupStack content a set of Stacks
type GroupStack struct {
	buffSize int64             // channel buffer size of stack
	poolSize int64             // max poolSize of stack
	discard  bool              // is discard new item when stack full
	closed   bool              // is group closed
	stacks   map[string]*Stack // Stack list, index by stack name
	mu       sync.Mutex        // op lock
}

// NewGroupStack return a new *GroupStack
// args will pass to GetStack when create new Stack
func NewGroupStack(chanBufferSize int64, poolSize int64, discard bool) *GroupStack {
	self := &GroupStack{
		buffSize: chanBufferSize,
		poolSize: poolSize,
		discard:  discard,
		closed:   false,
		stacks:   make(map[string]*Stack),
		mu:       sync.Mutex{},
	}
	return self
}

// Close all stacks in group
func (self *GroupStack) Close() {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.closed {
		return
	}
	for key, _ := range self.stacks {
		self.stacks.Close()
	}
	return
}

// GetStack return the Stack of key in group
// if the Stack no exist, new Stack created
func (self *GroupStack) GetStack(key string) *Stack {
	self.mu.Lock()
	defer self.mu.Unlock()
	if _, ok := self.stacks[key]; !ok {
		self.stacks[key] = NewStack(self.buffSize, self.poolSize, self.discard)
	}
	return self.stacks[key]
}

// DelStack close and remove the Stack of key in group
// if the Stack no exist, nothing happen
func (self *GroupStack) DelStack(key string) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if _, ok := self.stacks[key]; !ok {
		return
	}
	self.stacks[key].Close()
	delete(self.stacks, key)
	return
}

// FIFO queue

// TODO: Lock Free Stack

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
