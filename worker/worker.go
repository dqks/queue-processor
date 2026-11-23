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
			if !ok {
				select {
				case res, ok := <-retryChan:
					return res, ok
				default:
					return *new(J), false
				}
			}
			return res, ok
		default:
			return *new(J), false
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
		close(retryChan)
	}()

	for r := range resultChan {
		resultCb(r)
	}
}
