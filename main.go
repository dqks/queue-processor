package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"queue-processor/task"
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
	sendValue func(Result),
	sendRetry func(task.Task),
	readTask func() (task.Task, bool),
) {
	processTask := func(
		workerIndex int,
		job task.Task,
	) Result {
		var result Result
		if job.ID%2 == 0 {
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
		job, opened := readTask()

		// ctx.Err() != nil чтобы не взяли новую задачу в обработку при отмененном контексте
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
			if res.Status == "failed" && job.Attempt < 3 {
				job.Attempt += 1
				job.Wait = 1
				fmt.Printf("Новая попытка %d для задачи с ID %d\n", job.Attempt, job.ID)
				sendRetry(job)
			} else {
				sendValue(res)
			}
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

	tasks := task.NewList()

	worker.New(ctx, tasks, 3, processQueue, resultProcess)
}
