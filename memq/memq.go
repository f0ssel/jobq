package memq

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/f0ssel/jobq"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

// validate memoryTicketManager impliments jobq.TicketManager
var _ jobq.TicketManager = (*memoryTicketManager)(nil)

// validate memoryTicketManager impliments jobq.Scheduler
var _ jobq.Scheduler = (*memoryTicketManager)(nil)

type memoryTicketManager struct {
	mu     sync.Mutex
	topics map[string][]jobq.Ticket
}

func TicketManager() *memoryTicketManager {
	return &memoryTicketManager{
		topics: make(map[string][]jobq.Ticket),
	}
}

func (tm *memoryTicketManager) Pull(ctx context.Context, topics ...string) (jobq.Ticket, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, topic := range topics {
		tickets, ok := tm.topics[topic]
		if !ok {
			continue
		}

		for i, ticket := range tickets {
			if ticket.Phase == jobq.PhasePending {
				// pull ticket
				ticket.Phase = jobq.PhaseProcessing
				ticket.UpdatedAt = time.Now()

				tm.topics[topic][i] = ticket

				return ticket, nil
			}
		}
	}

	return jobq.Ticket{}, jobq.ErrNotFound
}

func (tm *memoryTicketManager) Update(ctx context.Context, t jobq.Ticket) (jobq.Ticket, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for i, ticket := range tm.topics[t.Topic] {
		if ticket.ID == ticket.ID {
			t.UpdatedAt = time.Now()

			tm.topics[t.Topic][i] = t

			return t, nil
		}
	}

	return jobq.Ticket{}, jobq.ErrNotFound
}

func (tm *memoryTicketManager) Schedule(ctx context.Context, r jobq.Request) (jobq.Job, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	b, err := json.Marshal(r.Body)
	if err != nil {
		return nil, xerrors.Errorf("marshal request body: %w", err)
	}

	t := jobq.Ticket{
		ID:        uuid.NewString(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Topic:     r.Topic,
		Phase:     jobq.PhasePending,
		Body:      b,
	}

	tm.topics[t.Topic] = append(tm.topics[t.Topic], t)

	return &memoryJob{
		t: t,
	}, nil
}

type memoryJob struct {
	t jobq.Ticket
}

func (j *memoryJob) Ticket(ctx context.Context) (jobq.Ticket, error) {
	return j.t, nil
}
