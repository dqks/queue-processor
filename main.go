package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"queue-processor/worker"
	"sync"
	"syscall"
	"time"
)

type Result struct {
	Status      string
	WorkerIndex int
	JobID       int
}

func process(
	ctx context.Context,
	workerIndex int,
	jobChan <-chan Task,
	resultChan chan<- Result, wg *sync.WaitGroup,
) {
	defer wg.Done()
	for job := range jobChan {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Printf("Начало работы воркера %d, задачи %d\n", workerIndex, job.ID)
			time.Sleep(time.Second * time.Duration(job.Wait))
			if workerIndex+job.ID >= 20 {
				resultChan <- Result{
					WorkerIndex: workerIndex,
					JobID:       job.ID,
					Status:      "failed",
				}
			} else {
				resultChan <- Result{
					WorkerIndex: workerIndex,
					JobID:       job.ID,
					Status:      "completed",
				}
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

	tasks := GetTaskList()

	worker.New(ctx, tasks, 3, process, resultProcess)

	<-ctx.Done()
	fmt.Println("сигнал получен")
}
