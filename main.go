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
	) Result {
		// time.Sleep(time.Second * time.Duration(job.Wait))
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
		return result
	}

	for {
		job, opened := readValue()
		if !opened || ctx.Err() != nil {
			return
		}
		fmt.Printf("Начало работы воркера %d, задачи %d\n", workerIndex, job.ID)
		select {
		case <-ctx.Done():
			sendValue(Result{
				WorkerIndex: workerIndex,
				JobID:       job.ID,
				Status:      "canceled",
			})
			fmt.Printf("Воркер %d отменился\n", workerIndex)
			return
		case <-time.After(time.Duration(job.Wait) * time.Second):
			// Проверка, потому что обе ветки могут быть готовы одновременно
			// И может выбраться именно эта ветка
			if ctx.Err() != nil {
				sendValue(Result{
					WorkerIndex: workerIndex,
					JobID:       job.ID,
					Status:      "canceled",
				})
				fmt.Printf("Воркер %d отменился\n", workerIndex)
				return
			}
			res := processTask(workerIndex, job)
			sendValue(res)
			fmt.Printf("Конец работы воркера %d, задачи %d\n", workerIndex, job.ID)
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
