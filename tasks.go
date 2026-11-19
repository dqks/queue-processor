package main

import (
	"context"
	"math/rand/v2"
	"strconv"
)

type Task struct {
	ID     int
	Name   string
	Wait   int
	Ctx    context.Context
	Cancel context.CancelFunc
}

// Возвращаем просто указатель потому что так принято и слайс уже содержит внутри себя указатель
// который указывает только на элементы, но копировать заголовки стоит дешево
func GetTaskList() []Task {
	tasks := make([]Task, 0, 20)

	for i := 1; i <= 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		wait := rand.IntN(3-1+1) + 1
		tasks = append(tasks, Task{
			ID:     i,
			Name:   "task " + strconv.Itoa(i),
			Wait:   wait,
			Ctx:    ctx,
			Cancel: cancel,
		})
	}

	return tasks
}
