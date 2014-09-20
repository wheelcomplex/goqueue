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

	"github.com/wheelcomplex/goqueue"
	"github.com/wheelcomplex/misc"
)

var mod *string       // -m
var loopNum *int      // -l
var cpuNum *int       // -c
var timeNum *int      // -t
var comsNum *int      // -C
var prodNum *int      // -P
var bufNum *int       // -B
var sizeNum *int      // -S
var discardFlag *bool // -D
var showFlag *bool    // -v

//var wg sync.WaitGroup

func runStack() {
	//
	incount := make(chan int64, 1024)
	outcount := make(chan int64, 1024)
	stack := goqueue.NewStack(int64(*bufNum), int64(*sizeNum), *discardFlag)
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
		if *showFlag {
			if i == 1 {
				fmt.Printf("%d-%d", i, stack.GetCacheSize())
			} else {
				fmt.Printf(" %d-%d", i, stack.GetCacheSize())
			}
		}
	}
	if *showFlag {
		fmt.Println("")
	}
	stack.Close()
	//close(stack.In())
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
	bufNum = flag.Int("B", 256, "buffer size of channel")
	sizeNum = flag.Int("S", 10000, "pool size of channel")
	discardFlag = flag.Bool("D", false, "discard flag")
	showFlag = flag.Bool("v", false, "show stat flag")
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
		//for i := 0; i < 2; i++ {
		//	misc.UUID()
		//}
	}
	return
}

func stackOut(stack *goqueue.Stack, count chan<- int64) {
	var inc int64
	out := stack.Out()
	//tk := time.NewTicker(500)
	//defer tk.Stop()
	for _ = range out {
		//for i := 0; i < 2; i++ {
		//	misc.UUID()
		//}
		//<-tk.C
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
		}
		return
	}()
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
	//// out
	go func() {
		for {
			_, ok := <-in
			if !ok {
				return
			}
			inc++
		}
		return
	}()
	// out
	//go func() {
	//	for _ = range in {
	//		inc++
	//	}
	//	return
	//}()

	//
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
	// QPS:
	// 562w with 128 buffer size
	// 255w with 0 buffer size
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
	// forward
	go func() {
		for {
			v, ok := <-in
			if !ok {
				return
			}
			out <- v
		}
		return
	}()
	// out
	var inc int64
	go func() {
		for {
			_, ok := <-out
			if !ok {
				return
			}
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
		for {
			_, ok := <-in
			if !ok {
				return
			}
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
