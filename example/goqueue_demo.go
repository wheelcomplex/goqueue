//
// goqueue demo
//

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	//"sync"
	"time"

	//"github.com/wheelcomplex/chanlogger"
	"github.com/wheelcomplex/goqueue"
	"github.com/wheelcomplex/misc"
)

//var l *chanlogger.Clogger

//func init() {
//	l = chanlogger.NewLogger()
//}

var mod *string       // -m
var loopNum *int      // -l
var cpuNum *int       // -c
var timeNum *int      // -t
var comsNum *int      // -C
var prodNum *int      // -P
var bufNum *int       // -B
var sizeNum *int      // -S
var discardFlag *bool // -D

//var wg sync.WaitGroup

func runStack() {
	/*
		// multi cpu
		160: -c 16 -C 4 -P 4
		300: -c 16 -C 1 -P 1

		// sigle cpu
		158: -t 30 -c 1 -P 1 -C 1 -B 0 -S 0 -D VS 247(run2)
		248: -c 1 -P 1 -C 1 -B 0 -S 0 VS 247(run2)

		550: -t 30 -c 1 -P 1 -C 1 -B 0 -S 0 -D VS 552(run2)
		558: -c 1 -P 1 -C 1 -B 1024 -S 0 VS 560(run2)

		600: -c 1 -C 1 -P 1
		500: -c 1 -C 1 -P 1
		500: -c 1 -C 4 -P 4
	*/
	incount := make(chan int64, 1024)
	outcount := make(chan int64, 1024)
	stack := goqueue.NewStack(int64(*bufNum), -int64(*sizeNum), *discardFlag)
	for i := 0; i < *prodNum; i++ {
		go stackIn(stack, incount)
	}
	for i := 0; i < *comsNum; i++ {
		go stackOut(stack, outcount)
	}
	var ti int64 = 0
	var to int64 = 0
	for i := int(1); i <= (*timeNum); i++ {
		time.Sleep(1e9)
		if i == 1 {
			fmt.Printf("%d", i)
		} else {
			fmt.Printf("-%d", i)
		}
	}
	fmt.Println("")
	stack.Close()
	totalcounter := *prodNum + *comsNum
	for totalcounter > 0 {
		select {
		case vi := <-incount:
			ti = ti + vi
			totalcounter--
		case vo := <-outcount:
			to = to + vo
			totalcounter--
		}
	}
	fmt.Println("runStack, cpu", runtime.GOMAXPROCS(-1), "discard", *discardFlag, "comsummer", *comsNum, "productor", *prodNum, " total in", ti, "in QPS", misc.RoundString(ti/int64(*timeNum), 10000), "total out", to, "out QPS", misc.RoundString(to/int64(*timeNum), 10000), "in - out =", ti-to)
	time.Sleep(1e9)
}

func main() {
	mod = flag.String("m", "stack", "run mod(stack/number/scan)")
	loopNum = flag.Int("l", 2, "loop number")
	cpuNum = flag.Int("c", 1, "loop number")
	timeNum = flag.Int("t", 30, "one loop time")
	prodNum = flag.Int("P", 1, "comsummer number")
	comsNum = flag.Int("C", 1, "productor number")
	bufNum = flag.Int("B", 0, "buffer size of channel")
	sizeNum = flag.Int("S", 0, "pool size of channel")
	discardFlag = flag.Bool("D", false, "discard flag")
	flag.Parse()
	if *cpuNum < 1 {
		*cpuNum = 1
	}
	if *loopNum < 1 {
		*loopNum = 1
	}
	runtime.GOMAXPROCS(*cpuNum)
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	switch *mod {
	case "stack":
		for i := 0; i < *loopNum; i++ {
			fmt.Println("--------------")
			//run1()
			//run2()
			//run3()
			//
			runStack()
			fmt.Println("--------------")
		}
	default:
		fmt.Printf("unknow module: %s\n", mod)
	}
}

//
func stackIn(stack *goqueue.Stack, count chan<- int64) {
	var inc int64
	in := stack.In()
	defer func() {
		recover()
		count <- inc
		//wg.Done()
	}()
	for {
		in <- struct{}{}
		inc++
	}
	return
}

func stackOut(stack *goqueue.Stack, count chan<- int64) {
	var inc int64
	out := stack.Out()
	for _ = range out {
		//for i := 0; i < 3; i++ {
		//	misc.UUID()
		//}
		inc++
	}
	count <- inc
	//wg.Done()
}

func run1() {
	// QPS: 12084224
	in := make(chan interface{}, *bufNum)

	var inc int64
	// in
	go func() {
		defer func() {
			recover()
		}()
		for {
			in <- struct{}{}
			//in <- nil
		}
		return
	}()
	// out
	go func() {
		for _ = range in {
			inc++
		}
		return
	}()
	for i := int(0); i < (*timeNum); i++ {
		time.Sleep(1e9)
		if i == 0 {
			fmt.Printf("%d", i)
		} else {
			fmt.Printf("-%d", i)
		}
	}
	close(in)
	fmt.Println("-|")
	to := inc
	fmt.Println("run1, cpu", runtime.GOMAXPROCS(-1), "total out", to, "out QPS", misc.RoundString(to/int64(*timeNum), 10000))
	time.Sleep(1e9)
	return
}

func run2() {
	// QPS: 5621964
	in := make(chan interface{}, *bufNum)
	out := make(chan interface{}, *bufNum)
	var to int64 = 0
	// in
	go func() {
		defer func() {
			recover()
		}()
		for {
			in <- struct{}{}
		}
		return
	}()
	//// forward
	//go func() {
	//	for v := range in {
	//		out <- v
	//	}
	//	return
	//}()
	// forward
	go func() {
		var ok bool
		var v interface{}
		for {
			select {
			case v, ok = <-in:
				if ok {
					out <- v
				} else {
					fmt.Printf("forwarder exit, in %v, ok %v\n", v, ok)
					return
				}
			}
		}
		return
	}()
	// out
	var inc int64
	go func() {
		for _ = range out {
			inc++
		}
		return
	}()
	for i := int(0); i < (*timeNum); i++ {
		time.Sleep(1e9)
		if i == 0 {
			fmt.Printf("%d", i)
		} else {
			fmt.Printf("-%d", i)
		}
	}
	close(in)
	fmt.Println("-|")
	to = inc
	fmt.Println("run2, cpu", runtime.GOMAXPROCS(-1), "total out", to, "out QPS", misc.RoundString(to/int64(*timeNum), 10000))
	time.Sleep(1e9)
	return
}

func run3() {
	// QPS: 12084224
	in := make(chan interface{}, *bufNum)
	var inc int64
	// in
	go func() {
		defer func() {
			recover()
		}()
		for {
			in <- nil
		}
		return
	}()
	// out
	go func() {
		for _ = range in {
			inc++
		}
		return
	}()
	for i := int(0); i < (*timeNum); i++ {
		time.Sleep(1e9)
		if i == 0 {
			fmt.Printf("%d", i)
		} else {
			fmt.Printf("-%d", i)
		}
	}
	close(in)
	fmt.Println("-|")
	to := inc
	fmt.Println("run3, cpu", runtime.GOMAXPROCS(-1), "total out", to, "out QPS", misc.RoundString(to/int64(*timeNum), 10000))
	time.Sleep(1e9)
	return
}

//
//
//
//
//
//
//
//
//
//
//
//
