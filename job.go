package jobq

import (
	"context"
	"time"
)

type Scheduler interface {
	Schedule(ctx context.Context, req Request) (Job, error)
}

type Worker interface {
	Work(ctx context.Context, body interface{}) error
}

type Job interface {
	Ticket(ctx context.Context) (Ticket, error)
}

type TicketManager interface {
	Pull(ctx context.Context, topic ...string) (Ticket, error)
	Update(ctx context.Context, t Ticket) (Ticket, error)
}

type Request struct {
	Topic string
	Body  interface{}
}

type Ticket struct {
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time
	StartedAt time.Time
	EndedAt   time.Time
	Topic     string
	Phase     Phase
	Body      []byte
	ErrorMsg  string
}

type Phase string

const (
	PhasePending    Phase = "PENDING"
	PhaseProcessing Phase = "PROCESSING"
	PhaseComplete   Phase = "COMPLETE"
)
