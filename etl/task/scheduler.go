// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/task/client"
	"github.com/echa/config"
	"github.com/echa/log"
)

type TaskCompletionCallback func(context.Context, *TaskResult) error

type Scheduler struct {
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	stop          chan struct{}
	done          chan struct{}
	stopTimeout   time.Duration
	log           log.Logger
	cb            TaskCompletionCallback
	maxTasks      int
	retryInterval time.Duration
	taskq         chan struct{}
	taskLimiter   *time.Ticker
	table         *pack.Table
	client        *client.Client
}

func NewScheduler() *Scheduler {
	maxTasks := config.GetInt("meta.max_tasks")
	rateLimit := config.GetInt("meta.http.rate_limit")
	if rateLimit <= 0 {
		rateLimit = int(time.Second)
	}
	s := &Scheduler{
		stop:          make(chan struct{}),
		stopTimeout:   10 * time.Second,
		log:           log.Log,
		maxTasks:      maxTasks,
		taskq:         make(chan struct{}, maxTasks),
		taskLimiter:   time.NewTicker(time.Second / time.Duration(rateLimit)),
		retryInterval: config.GetDuration("meta.http.retry_interval"),
		client:        client.New(),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	for i := maxTasks; i > 0; i-- {
		s.taskq <- struct{}{}
	}
	return s
}

func (s *Scheduler) WithTable(t *pack.Table) *Scheduler {
	s.table = t
	return s
}

func (s *Scheduler) WithCallback(cb TaskCompletionCallback) *Scheduler {
	s.cb = cb
	return s
}

func (s *Scheduler) SetLogLevel(lvl log.Level) {
	s.log.SetLevel(lvl)
	s.client.SetLogLevel(lvl)
}

func (s *Scheduler) WithLogger(logger log.Logger) *Scheduler {
	s.log = logger
	s.client.WithLogger(logger)
	return s
}

func (s *Scheduler) WithContext(ctx context.Context) *Scheduler {
	// cancel the current context
	s.cancel()
	// create new sub-context
	s.ctx, s.cancel = context.WithCancel(ctx)
	return s
}

func (s *Scheduler) Start() {
	s.log.Info("Starting Task Sechuler")

	// reset pending requests
	var (
		last  uint64
		count int
		list  []TaskRequest
	)
	for {
		err := pack.NewQuery("reset_tasks").
			WithTable(s.table).
			WithLimit(s.maxTasks).
			AndGt("id", last).
			AndEqual("status", TaskStatusRunning).
			Execute(s.ctx, &list)
		if err != nil {
			s.log.Errorf("reset pending tasks: %v", err)
			break
		}
		if len(list) == 0 {
			break
		}
		for _, v := range list {
			v.Status = TaskStatusIdle
			if err := s.table.Update(s.ctx, &v); err != nil {
				s.log.Errorf("reset T_%d %s: %v", v.Id, v.Owner, err)
				break
			}
			count++
			last = v.Id
		}
		list = list[:0]
	}
	s.log.Debugf("Reset %d tasks", count)
	s.done = make(chan struct{})

	// run auto-retries
	s.wg.Add(1)
	go s.runLoop()
}

func (s *Scheduler) Stop() {
	s.log.Info("Stopping Task Scheduler")

	// graceful shutdown
	close(s.stop)
	select {
	case <-s.done:
	case <-time.After(s.stopTimeout):
		s.log.Warnf("Killing Task Scheduler")
	}

	// force-stop main loop
	s.cancel()
	s.wg.Wait()

	// free rate limiter
	s.taskLimiter.Stop()

	// shutdown sources, will close channels
	s.stop = nil
	s.done = nil

	s.log.Info("Done Task Scheduler")
}

func (s *Scheduler) Run(req TaskRequest) error {
	// init
	req.Status = TaskStatusIdle

	// store task in DB, generates unique id
	return s.table.Insert(s.ctx, &req)
}

func (s *Scheduler) execute(r TaskRequest) error {
	// hard-limit number of running async tasks
	select {
	case <-s.taskq:
	case <-s.stop:
		return ErrShuttingDown
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
		return ErrAgain
	}

	r.Status = TaskStatusRunning
	if err := s.table.Update(s.ctx, &r); err != nil {
		return fmt.Errorf("T_%d %s store: %v", r.Id, r.Owner, err)
	}

	// run in separate goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// limit time between tasks
		select {
		case <-s.taskLimiter.C:
		case <-s.stop:
			return
		case <-s.ctx.Done():
			return
		}
		// fetch URL
		r := s.fetch(r)

		// check for shutdown
		select {
		case <-s.stop:
			return
		case <-s.ctx.Done():
			return
		default:
		}

		// store result for later delivery
		if r.Status != TaskStatusRunning {
			if err := s.table.Update(s.ctx, &r); err != nil {
				s.log.Warnf("T_%d %s store: %v", r.Id, r.Owner, err)
			}
		}

		// signal room for one more task
		s.taskq <- struct{}{}
	}()

	return nil
}

func (s *Scheduler) runLoop() {
	defer s.wg.Done()
	defer close(s.done)
	list := make([]TaskRequest, 0, s.maxTasks)
	for {
		// exit on shutdown, retry
		select {
		case <-s.stop:
			return
		case <-s.ctx.Done():
			return
		case <-time.After(s.retryInterval):
		}

		// run idle tasks
		err := pack.NewQuery("idle_tasks").
			WithTable(s.table).
			WithLimit(s.maxTasks).
			AndEqual("status", TaskStatusIdle).
			Execute(s.ctx, &list)
		if err != nil {
			s.log.Warnf("scheduler: %v", err)
			continue
		}

		for _, v := range list {
			err := s.execute(v)
			if err != nil {
				if err != ErrAgain {
					s.log.Warnf("scheduler: %v", err)
				}
				break
			}
		}
		list = list[:0]

		// deliver complete tasks
		err = pack.NewQuery("complete_tasks").
			WithTable(s.table).
			WithLimit(s.maxTasks).
			AndIn("status", []TaskStatus{
				TaskStatusSuccess,
				TaskStatusFailed,
				TaskStatusTimeout,
			}).
			Execute(s.ctx, &list)
		if err != nil {
			s.log.Warnf("scheduler: %v", err)
			continue
		}

		for _, v := range list {
			_ = s.tryDeliver(v)
		}
		list = list[:0]
	}
}

func (s *Scheduler) fetch(r TaskRequest) TaskRequest {
	s.log.Debugf("T_%d %s fetch: %s", r.Id, r.Owner, r.Url)
	buf, err := s.client.Get(s.ctx, r.Url)
	if err != nil {
		s.log.Warnf("T_%d %s fetch: %v", r.Id, r.Owner, err)
	}
	if err != nil {
		switch {
		case errors.Is(err, client.ErrNetwork), errors.Is(err, client.ErrPermanent):
			r.Status = TaskStatusFailed
		case errors.Is(err, client.ErrTimeout):
			r.Status = TaskStatusTimeout
		default:
			r.Status = TaskStatusFailed
		}
	} else {
		r.Status = TaskStatusSuccess
		r.Data = buf
	}
	return r
}

func (s *Scheduler) tryDeliver(r TaskRequest) error {
	if r.Ordered {
		// ensure serial consistency with other requests, deliver results in-order
		count, err := pack.NewQuery("task_serial").
			WithTable(s.table).
			AndLt("id", r.Id).
			AndEqual("owner", r.Owner).
			Count(s.ctx)
		if err != nil {
			return err
		}
		if count > 0 {
			s.log.Debugf("T_%d %s deliver: %d pending try again", r.Id, r.Owner, count)
			return ErrAgain
		}
	}

	// have the consumer acknowledge this result before deletion
	res := &TaskResult{
		Id:      r.Id,
		Index:   r.Index,
		Decoder: r.Decoder,
		Owner:   r.Owner,
		Account: r.Account,
		Flags:   r.Flags,
		Status:  r.Status,
		Data:    r.Data,
		Url:     r.Url,
	}
	if err := s.cb(s.ctx, res); err != nil {
		s.log.Errorf("T_%d %s deliver: %v", r.Id, r.Owner, err)
	} else {
		err := s.table.DeleteIds(s.ctx, []uint64{r.Id})
		if err != nil {
			return err
		}
	}
	return nil
}
