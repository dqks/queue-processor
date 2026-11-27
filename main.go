package main

import (
	"context"
	"fmt"
	"math/rand/v2"
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
		status := [2]string{"failed", "completed"}
		result := Result{
			WorkerIndex: workerIndex,
			JobID:       job.ID,
			Status:      status[rand.IntN(2)],
		}
		return result
	}

	for {
		job, opened := readTask()

		if !opened {
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
			fmt.Printf("Воркер %d досрочно пытается завершить работу\n", workerIndex)
		case <-time.After(time.Duration(job.Wait) * time.Second):
			// Проверка, потому что обе ветки могут быть готовы одновременно
			// И может выбраться именно эта ветка
			if ctx.Err() != nil {
				sendValue(Result{
					WorkerIndex: workerIndex,
					JobID:       job.ID,
					Status:      "canceled",
				})
				fmt.Printf("Воркер %d досрочно пытается завершить работу\n", workerIndex)
			} else {
				res := processTask(workerIndex, job)
				if res.Status == "failed" && job.Attempt < 3 {
					select {
					case <-time.After(1 * time.Second):
						if ctx.Err() != nil {
							res.Status = "canceled"
							sendValue(res)
						} else {
							job.Attempt += 1
							fmt.Printf("Новая попытка %d для задачи с ID %d\n", job.Attempt, job.ID)
							sendRetry(job)
						}
					case <-ctx.Done():
						res.Status = "canceled"
						sendValue(res)
					}
				} else {
					sendValue(res)
				}
				fmt.Printf("Конец работы воркера %d, задачи %d\n", workerIndex, job.ID)
			}
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

	tasks := task.NewList(20, rand.IntN(3-1+1)+1)
	// stop()
	worker.New(ctx, tasks, 3, processQueue, resultProcess)
}
