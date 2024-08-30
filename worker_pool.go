package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync/atomic"
)

type Task struct {
	taskId *uint64
	RunFn  func() (result interface{}, err error)
}

type WorkerPool struct {
	ctx     context.Context
	group   *errgroup.Group
	tasks   map[uint64]*Task
	started atomic.Bool
	lock    sync.Mutex
}

type Result struct {
	Task   *Task
	Result interface{}
	Err    error
}

type Results chan *Result

func NewWorkerPool(ctx context.Context, workerNum int) *WorkerPool {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(workerNum)
	workerPool := WorkerPool{ctx: ctx, group: errGroup, tasks: make(map[uint64]*Task)}
	return &workerPool
}

func (pool *WorkerPool) AddTask(task *Task) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if pool.started.Load() {
		return errors.New("Can't Add Task: Pool already started")
	}
	if task.taskId == nil {
		tmp := uint64(uuid.New().ID())
		task.taskId = &tmp
	}

	pool.tasks[*task.taskId] = task
	return nil
}

func (pool *WorkerPool) GetAllTasks() []*Task {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	tasks := make([]*Task, 0, len(pool.tasks))
	for _, task := range pool.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

func (pool *WorkerPool) RemoveTaskByID(taskID uint64) error {
	if _, ok := pool.tasks[taskID]; !ok {
		return errors.New("Task ID not found")
	}
	delete(pool.tasks, taskID)
	return nil
}

func (pool *WorkerPool) Run() (resultChan Results, err error) {
	if pool.started.Load() {
		return nil, errors.New("Can't Run: Pool already started")
	}

	if len(pool.tasks) == 0 {
		return nil, errors.New("Can't Run: No Tasks")
	}

	resultChan = make(chan *Result, len(pool.tasks))

	pool.started.Store(true)
	for _, task := range pool.tasks {
		task := task
		pool.group.Go(func() error {
			res, err := task.RunFn()
			result := &Result{Task: task, Result: res, Err: err}
			resultChan <- result
			return nil
		})
	}
	pool.group.Wait()
	close(resultChan)
	return resultChan, nil
}

func (pool *WorkerPool) Reset() {
	pool.started.Store(false)
	pool.tasks = make(map[uint64]*Task)
}

func (pool *WorkerPool) Rerun() (resultChan Results, err error) {
	pool.started.Store(false)
	return pool.Run()
}
