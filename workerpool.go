package jobq

import (
	"context"
	"sync"
	"time"
)

func NewWorkerPool(tm TicketManager) *WorkerPool {
	return &WorkerPool{
		tm:      tm,
		jobs:    make(map[string]chan Ticket),
		workers: make(map[string][]Worker),
	}
}

type WorkerPool struct {
	tm       TicketManager
	jobs     map[string]chan Ticket
	workers  map[string][]Worker
	workerWg sync.WaitGroup
	workerMu sync.Mutex
}

func (wp *WorkerPool) Handle(topic string, workers []Worker) {
	wp.workerMu.Lock()
	defer wp.workerMu.Unlock()

	wp.workers[topic] = workers
}

func (wp *WorkerPool) topics() []string {
	topics := []string{}
	for k, _ := range wp.jobs {
		topics = append(topics, k)
	}

	return topics
}

func (wp *WorkerPool) Run(ctx context.Context) error {
	wp.startWorkers(ctx)
	wp.pullTickets(ctx)

	return nil
}

func (wp *WorkerPool) startWorkers(ctx context.Context) {
	for t, wks := range wp.workers {
		for _, w := range wks {
			go wp.runWorker(ctx, t, w)
		}
	}
}

func (wp *WorkerPool) runWorker(ctx context.Context, topic string, w Worker) {
	wp.workerWg.Add(1)
	defer wp.workerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-wp.jobs[topic]:
			t.StartedAt = time.Now()
			t, err := wp.tm.Update(ctx, t)
			if err != nil {
				continue
			}

			err = w.Work(ctx, t)
			if err != nil {
				t.ErrorMsg = err.Error()
			}

			t.Phase = PhaseComplete
			t.EndedAt = time.Now()
			t, err = wp.tm.Update(ctx, t)
			if err != nil {
				continue
			}
		}
	}
}

func (wp *WorkerPool) pullTickets(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			t, err := wp.tm.Pull(ctx, wp.topics()...)
			if err != nil {
				continue
			}

			wp.jobs[t.Topic] <- t
		}
	}
}
