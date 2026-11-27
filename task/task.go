package task

import (
	"strconv"
)

type Task struct {
	ID      int
	Name    string
	Wait    int
	Attempt int
}

// Возвращаем просто указатель потому что так принято и слайс уже содержит внутри себя указатель
// который указывает только на элементы, но копировать заголовки стоит дешево
func NewList(n int, wait int) []Task {
	tasks := make([]Task, 0, 20)

	for i := 1; i <= n; i++ {
		wait := wait
		tasks = append(tasks, Task{
			ID:      i,
			Name:    "task " + strconv.Itoa(i),
			Wait:    wait,
			Attempt: 1,
		})
	}

	return tasks
}
