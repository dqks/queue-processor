package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"queue-processor/worker"
	"syscall"
	"time"
)

type Result struct {
	Status      string
	WorkerIndex int
	JobID       int
}

func processQueue(
	ctx context.Context,
	workerIndex int,
	readValue func() (Task, bool),
	sendValue func(value Result),
) {
	processTask := func(
		workerIndex int,
		job Task,
	) chan Result {
		resChan := make(chan Result)
		go func() {
			fmt.Printf("Начало работы воркера %d, задачи %d\n", workerIndex, job.ID)
			time.Sleep(time.Second * time.Duration(job.Wait))
			var result Result
			if workerIndex+job.ID >= 20 {
				result = Result{
					WorkerIndex: workerIndex,
					JobID:       job.ID,
					Status:      "failed",
				}
			} else {
				result = Result{
					WorkerIndex: workerIndex,
					JobID:       job.ID,
					Status:      "completed",
				}
			}
			resChan <- result
			fmt.Printf("Конец работы воркера %d, задачи %d\n", workerIndex, job.ID)
		}()
		return resChan
	}

	for {
		job, opened := readValue()
		if !opened {
			return
		}
		select {
		case <-ctx.Done():
			fmt.Println("Воркеры отменились")
			return
		case res := <-processTask(workerIndex, job):
			sendValue(res)
		}
	}
}

func resultProcess(result Result) {
	fmt.Println(result)
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer stop()

	tasks := GetTaskList()

	worker.New(ctx, tasks, 3, processQueue, resultProcess)

	stop()
}
