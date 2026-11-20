package worker

import (
	"context"
	"fmt"
	"sync"
)

func New[J any, R any](
	ctx context.Context,
	jobs []J,
	workerCount int,
	processCb func(
		ctx context.Context,
		workerIndex int,
		sendValue func(R),
		sendRetry func(value J),
		readTask func() (J, bool),
	),
	resultCb func(result R),
) {
	jobChan := make(chan J, len(jobs))
	resultChan := make(chan R, len(jobs))
	retryChan := make(chan J, len(jobs)*3)
	var wg sync.WaitGroup

	readTask := func() (J, bool) {
		select {
		case res, opened := <-retryChan:
			fmt.Println("Из ретрая")
			return res, opened
		case res, opened := <-jobChan:
			fmt.Println("Из работ")
			return res, opened
		}
	}

	sendValue := func(value R) {
		resultChan <- value
	}

	sendRetry := func(value J) {
		retryChan <- value
	}

	processJobs := func(i int) {
		defer wg.Done()
		processCb(
			ctx,
			i,
			sendValue,
			sendRetry,
			readTask,
		)
	}

	for i := range workerCount {
		wg.Add(1)
		go processJobs(i)
	}

	for i := range len(jobs) {
		jobChan <- jobs[i]
	}
	close(jobChan)

	go func() {
		wg.Wait()
		close(resultChan)
		close(retryChan)
	}()

	for r := range resultChan {
		resultCb(r)
	}
}
