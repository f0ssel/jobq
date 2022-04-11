package jobq

import (
	"context"
	"time"
)

func Wait(ctx context.Context, interval time.Duration, j Job) error {
	t := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			ticket, err := j.Ticket(ctx)
			if err != nil {
				return err
			}

			if ticket.Phase == PhaseComplete {
				return nil
			}
		}
	}
}
