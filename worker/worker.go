package worker

import (
	"context"
	"sync"
)

func New[J any, R any](
	ctx context.Context,
	jobs []J,
	workerCount int,
	processCb func(ctx context.Context, workerIndex int, jobChan <-chan J, resultChan chan<- R, wg *sync.WaitGroup),
	resultCb func(result R),
) {
	jobChan := make(chan J, len(jobs))
	resultChan := make(chan R, len(jobs))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go processCb(ctx, i, jobChan, resultChan, &wg)
	}

	for i := 0; i < len(jobs); i++ {
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
