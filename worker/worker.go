package worker

import (
	"context"
	"sync"
	"sync/atomic"
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
	if len(jobs) == 0 {
		return
	}

	jobChan := make(chan J, len(jobs))
	retryChan := make(chan J, len(jobs)*2)
	resultChan := make(chan R, len(jobs))
	var processedJobs atomic.Int64
	var wg sync.WaitGroup

	sendValue := func(value R) {
		resultChan <- value
	}

	sendRetry := func(value J) {
		retryChan <- value
	}

	readTask := func() (J, bool) {
		select {
		case res, ok := <-retryChan:
			return res, ok
		case res, ok := <-jobChan:
			return res, ok
		case <-ctx.Done():
			return *new(J), false
		}
	}

	for i := range len(jobs) {
		jobChan <- jobs[i]
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

	go func() {
		wg.Wait()
		close(resultChan)
		close(jobChan)
	}()

	for r := range resultChan {
		processedJobs.Add(1)

		if int(processedJobs.Load()) == len(jobs) {
			close(retryChan)
		}

		resultCb(r)
	}
}
