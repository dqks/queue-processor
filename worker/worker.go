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
	if len(jobs) == 0 || workerCount == 0 {
		return
	}

	jobChan := make(chan J, len(jobs))
	retryChan := make(chan J, len(jobs)*2)
	resultChan := make(chan R, len(jobs))
	retryClosed := false
	var processedJobs = 0
	var sentJobs atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex

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
			mu.Lock()
			if ctx.Err() != nil {
				if sentJobs.Load() == 0 {
					mu.Unlock()
					return *new(J), false
				}
				if sentJobs.Load() == int64(processedJobs) && !retryClosed {
					retryClosed = true
					close(retryChan)
				}
				mu.Unlock()
				res, ok := <-retryChan
				return res, ok
			}
			sentJobs.Add(1)
			mu.Unlock()
			return res, ok
		case <-ctx.Done():
			// Если контекст отменяется и не отправлено ни одной работы
			if sentJobs.Load() == 0 {
				return *new(J), false
			}
			mu.Lock()
			if sentJobs.Load() == int64(processedJobs) && !retryClosed {
				retryClosed = true
				close(retryChan)
			}
			mu.Unlock()
			res, ok := <-retryChan
			return res, ok
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
		mu.Lock()
		processedJobs += 1
		if (processedJobs == len(jobs) && !retryClosed) ||
			(sentJobs.Load() == int64(processedJobs) && ctx.Err() != nil && !retryClosed) {
			retryClosed = true
			close(retryChan)
		}
		mu.Unlock()

		resultCb(r)
	}
}
