package worker

import (
	"context"
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
	retryChan := make(chan J, len(jobs)*2)
	resultChan := make(chan R, len(jobs))
	countChan := make(chan struct{}, len(jobs))
	var wg sync.WaitGroup

	sendValue := func(value R) {
		resultChan <- value
	}

	sendRetry := func(value J) {
		retryChan <- value
	}

	readTask := func() (J, bool) {
		if len(jobChan) != 0 {
			select {
			case res, _ := <-retryChan:
				return res, !(len(countChan) == len(jobs))
			case res, _ := <-jobChan:
				return res, !(len(countChan) == len(jobs))
			case <-ctx.Done():
				return *new(J), false
			}
		} else {
			select {
			case res, _ := <-retryChan:
				return res, !(len(countChan) == len(jobs))
			case <-ctx.Done():
				return *new(J), false
			}
		}
	}

	for i := range len(jobs) {
		jobChan <- jobs[i]
	}
	close(jobChan)

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

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for r := range resultChan {
		countChan <- struct{}{}
		if len(countChan) == len(jobs) {
			close(retryChan)
		}
		resultCb(r)
	}
}
