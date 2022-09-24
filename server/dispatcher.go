// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

// A buffered channel that we can send work requests on.
var jobQueue chan *Context

type Worker struct {
	WorkerPool chan chan *Context
	JobChannel chan *Context
	quit       chan bool
}

func NewWorker(workerPool chan chan *Context) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan *Context),
		quit:       make(chan bool)}
}

func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case req := <-w.JobChannel:
				// check if the context is expired
				select {
				case <-req.Context.Done():
					err := req.Context.Err()
					req.handleError(err)
					req.sendResponse()
					// req.Logf("%s request skipped (%v)", req.RequestID, err)

				default:
					// req.Logf("%s serving...", req.RequestID)
					req.serve()
					req.sendResponse()
					// req.Logf("%s request served", req.RequestID)
					// req.Logf("%s worker done.", req.RequestID)
				}
				req.done <- nil

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	pool       chan chan *Context
	maxWorkers int
	maxQueue   int
}

func NewDispatcher(maxWorkers int, maxQueue int) *Dispatcher {
	jobQueue = make(chan *Context, maxQueue)
	pool := make(chan chan *Context, maxWorkers)
	return &Dispatcher{pool: pool, maxWorkers: maxWorkers, maxQueue: maxQueue}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.pool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		api := <-jobQueue
		// try to obtain a worker job channel that is available.
		// will block until a worker is idle or pool is closed
		workerChannel := <-d.pool

		// dispatch the job to the worker job channel
		if workerChannel != nil {
			workerChannel <- api
		}
		// api.Logf("%s dispatched\n", api.RequestID)
	}
}
