package worker

import (
	"context"
	"sync"
)

func New[J any, R any](
	ctx context.Context,
	jobs []J,
	workerCount int,
	processCb func(ctx context.Context, workerIndex int, readValue func() (J, bool), sendValue func(R), wg *sync.WaitGroup),
	resultCb func(result R),
) {
	jobChan := make(chan J, len(jobs))
	resultChan := make(chan R, len(jobs))
	var wg sync.WaitGroup

	readValue := func() (J, bool) {
		val, opened := <-jobChan
		return val, opened
	}

	sendValue := func(value R) {
		resultChan <- value
	}

	for i := range workerCount {
		wg.Add(1)
		go processCb(ctx, i, readValue, sendValue, &wg)
	}

	for i := range len(jobs) {
		jobChan <- jobs[i]
	}

	close(jobChan)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for r := range resultChan {
		resultCb(r)
	}
}
