package jobq_test

import (
	"context"
	"testing"
	"time"

	"github.com/f0ssel/jobq"
	"github.com/f0ssel/jobq/memq"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type mockWorker struct {
	fn func(ctx context.Context, body interface{}) error
}

func (w *mockWorker) Work(ctx context.Context, body interface{}) error {
	return w.fn(ctx, body)
}

func TestWorkerPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	mq := memq.TicketManager()
	wp := jobq.NewWorkerPool(mq)
	workStarted := make(chan struct{})
	workDone := make(chan struct{})
	worker := mockWorker{
		fn: func(ctx context.Context, body interface{}) error {
			workStarted <- struct{}{}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-workDone:
				return nil
			}
		},
	}
	topic := uuid.NewString()
	wp.Handle(topic, []jobq.Worker{&worker})
	go wp.Run(ctx)

	before := time.Now()
	j, err := mq.Schedule(ctx, jobq.Request{
		Topic: topic,
	})
	require.NoError(t, err)

	ticket, err := j.Ticket(ctx)
	require.NoError(t, err)
	require.True(t, ticket.CreatedAt.After(before))
	require.True(t, ticket.UpdatedAt.After(before))

	// wait for work to start
	<-workStarted
	ticket, err = j.Ticket(ctx)
	require.NoError(t, err)
	require.True(t, ticket.StartedAt.After(before))
	require.EqualValues(t, ticket.Phase, jobq.PhaseProcessing)

	// send signal to complete job
	workDone <- struct{}{}
	require.Eventually(t, func() bool {
		ticket, err = j.Ticket(ctx)
		require.NoError(t, err)
		return ticket.Phase == jobq.PhaseComplete
	}, time.Second, 10*time.Millisecond)
}
