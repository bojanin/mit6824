package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}
	var wg sync.WaitGroup
	wg.Add(ntasks)

	failuresChan := make(chan int)
	run := func(i int) {
		w := <-registerChan
		t := DoTaskArgs{jobName, mapFiles[i], phase, i, nOther}
		if call(w, "Worker.DoTask", t, nil) {
			wg.Done()
		} else {
			failuresChan <- i
		}
		registerChan <- w
	}
	for i := 0; i < ntasks; i++ {
		go run(i)
	}

	go func() {
		for {
			i := <-failuresChan
			go run(i)
		}
	}()

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
