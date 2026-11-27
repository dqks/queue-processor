package worker_test

import (
	"context"
	"queue-processor/task"
	"queue-processor/worker"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type Result struct {
	Status      string
	WorkerIndex int
	JobID       int
}

func NewTaskList(n int, wait int) []task.Task {
	tasks := make([]task.Task, 0, 20)

	for i := 1; i <= n; i++ {
		wait := wait
		tasks = append(tasks, task.Task{
			ID:      i,
			Name:    "task " + strconv.Itoa(i),
			Wait:    wait,
			Attempt: 1,
		})
	}

	return tasks
}

func TestNewSuccessfullProcessing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		tasks                    []task.Task
		workersCount             int
		wantProcessTaskCallCount int
		wantResultCbCallCount    int
	}{
		{
			name:                     "success",
			tasks:                    NewTaskList(20, 0),
			workersCount:             3,
			wantProcessTaskCallCount: 20,
			wantResultCbCallCount:    20,
		},
		{
			name:                     "success",
			tasks:                    NewTaskList(20, 0),
			workersCount:             1,
			wantProcessTaskCallCount: 20,
			wantResultCbCallCount:    20,
		},
		{
			name:                     "success",
			tasks:                    NewTaskList(5, 0),
			workersCount:             10,
			wantProcessTaskCallCount: 5,
			wantResultCbCallCount:    5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			var processCbCallCount atomic.Int64
			var processTaskCount atomic.Int64
			resultCbCallCount := 0

			processCb := func(
				ctx context.Context,
				workerIndex int,
				sendValue func(Result),
				sendRetry func(value task.Task),
				readTask func() (task.Task, bool)) {
				processCbCallCount.Add(1)
				processTask := func(
					workerIndex int,
					job task.Task,
				) Result {
					processTaskCount.Add(1)
					result := Result{
						WorkerIndex: workerIndex,
						JobID:       job.ID,
						Status:      "completed",
					}
					return result
				}

				for {
					job, opened := readTask()

					if !opened {
						return
					}

					// fmt.Printf("Начало работы воркера %d, задачи %d\n", workerIndex, job.ID)
					select {
					case <-ctx.Done():
						sendValue(Result{
							WorkerIndex: workerIndex,
							JobID:       job.ID,
							Status:      "canceled",
						})
						// fmt.Printf("Воркер %d досрочно пытается завершить работу\n", workerIndex)
					case <-time.After(time.Duration(job.Wait) * time.Second):
						// Проверка, потому что обе ветки могут быть готовы одновременно
						// И может выбраться именно эта ветка
						if ctx.Err() != nil {
							sendValue(Result{
								WorkerIndex: workerIndex,
								JobID:       job.ID,
								Status:      "canceled",
							})
							// fmt.Printf("Воркер %d досрочно пытается завершить работу\n", workerIndex)
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
										// fmt.Printf("Новая попытка %d для задачи с ID %d\n", job.Attempt, job.ID)
										sendRetry(job)
									}
								case <-ctx.Done():
									res.Status = "canceled"
									sendValue(res)
								}
							} else {
								sendValue(res)
							}
							// fmt.Printf("Конец работы воркера %d, задачи %d\n", workerIndex, job.ID)
						}
					}
				}
			}
			resultSlice := make([]Result, 0, len(tt.tasks))

			resultCb := func(r Result) {
				resultCbCallCount += 1
				resultSlice = append(resultSlice, r)
			}

			done := make(chan struct{})

			go func() {
				worker.New(
					ctx,
					tt.tasks,
					tt.workersCount,
					processCb,
					resultCb,
				)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatalf("function didn't finish in 3 seconds")
			}

			if tt.workersCount != int(processCbCallCount.Load()) {
				t.Errorf(
					"expected workers count to be %d but got %d",
					tt.workersCount,
					processCbCallCount.Load(),
				)
			}

			if tt.wantProcessTaskCallCount != int(processTaskCount.Load()) {
				t.Errorf(
					"expected process task count to be %d but got %d",
					tt.wantProcessTaskCallCount,
					processTaskCount.Load(),
				)
			}

			if resultCbCallCount != tt.wantResultCbCallCount {
				t.Errorf(
					"expected result cb count to be %d but got %d",
					tt.wantResultCbCallCount,
					resultCbCallCount,
				)
			}

			if len(resultSlice) != len(tt.tasks) {
				t.Errorf(
					"expected len of results to be %d but got %d",
					len(tt.tasks),
					len(resultSlice),
				)
			}

			for _, r := range resultSlice {
				if r.Status != "completed" {
					t.Errorf(
						"expected status of result to be %s but got %s",
						"completed",
						r.Status,
					)
				}
			}

			resultMap := map[int]int{}
			for _, r := range resultSlice {
				resultMap[r.JobID] = resultMap[r.JobID] + 1
			}

			taskMap := map[int]int{}
			for _, t := range tt.tasks {
				taskMap[t.ID] = taskMap[t.ID] + 1
			}

			if !reflect.DeepEqual(resultMap, taskMap) {
				t.Errorf(
					"expected result's ids to be %v\n but got %v",
					taskMap,
					resultMap,
				)
			}
		})
	}
}

func TestNewZeroJobs(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "no jobs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			jobs := make([]int, 0)

			var processCbCallCount atomic.Int64
			var resultCbCallCount int

			processCb := func(
				ctx context.Context,
				workerIndex int,
				sendValue func(int),
				sendRetry func(value int),
				readTask func() (int, bool)) {
				processCbCallCount.Add(1)
			}

			resultCb := func(n int) {
				resultCbCallCount += 1
			}

			done := make(chan struct{})
			go func() {
				worker.New(ctx, jobs, 3, processCb, resultCb)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatalf("function didn't finish in 3 seconds")
			}

			if processCbCallCount.Load() != 0 {
				t.Errorf(
					"expected not to call processCb but called %d times",
					processCbCallCount.Load(),
				)
			}

			if resultCbCallCount != 0 {
				t.Errorf(
					"expected not to call resultCb but called %d times",
					resultCbCallCount,
				)
			}
		})
	}
}

func TestNewZeroWorkers(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "no workers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			jobs := []int{1, 2, 3, 4, 5}

			var processCbCallCount atomic.Int64
			var resultCbCallCount int

			processCb := func(
				ctx context.Context,
				workerIndex int,
				sendValue func(int),
				sendRetry func(value int),
				readTask func() (int, bool)) {
				processCbCallCount.Add(1)
			}

			resultCb := func(n int) {
				resultCbCallCount += 1

			}

			done := make(chan struct{})
			go func() {
				worker.New(ctx, jobs, 0, processCb, resultCb)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatalf("function didn't finish in 3 seconds")
			}

			if processCbCallCount.Load() != 0 {
				t.Errorf(
					"expected not to call processCb but called %d times",
					processCbCallCount.Load(),
				)
			}

			if resultCbCallCount != 0 {
				t.Errorf(
					"expected not to call resultCb but called %d times",
					resultCbCallCount,
				)
			}
		})
	}
}
