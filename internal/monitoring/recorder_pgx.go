//go:build monitoring

package monitoring

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type pgxRecorder struct {
	pool   *pgxpool.Pool
	bus    *EventBus
	done   chan struct{}
}

func NewRecorder(bus *EventBus) Recorder {
	return &pgxRecorder{
		bus:  bus,
		done: make(chan struct{}),
	}
}

func (r *pgxRecorder) Start(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, "postgres://localhost:5432/kervan?sslmode=disable")
	if err != nil {
		return fmt.Errorf("monitoring: unable to connect to postgres: %w", err)
	}
	r.pool = pool

	if err := r.runMigrations(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("monitoring: migration failed: %w", err)
	}

	go r.batchLoop(ctx)
	return nil
}

func (r *pgxRecorder) Stop() error {
	if r.pool != nil {
		r.pool.Close()
	}
	close(r.done)
	return nil
}

func (r *pgxRecorder) batchLoop(ctx context.Context) {
	const (
		batchSize    = 100
		flushInterval = time.Second
	)

	var events []Event
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(events) == 0 {
			return
		}
		if err := r.flushEvents(ctx, events); err != nil {
			log.Printf("monitoring: flush error: %v", err)
		}
		events = events[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case evt, ok := <-r.bus.Subscribe():
			if !ok {
				flush()
				return
			}
			events = append(events, evt)
			if len(events) >= batchSize {
				flush()
			}
		}
	}
}

func (r *pgxRecorder) flushEvents(ctx context.Context, events []Event) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, evt := range events {
		metadata, ok := evt.Payload.(map[string]any)
		if !ok {
			continue
		}

		pid, _ := metadata["pipeline_id"].(string)
		if pid == "" {
			pid = "unknown"
		}

		switch evt.Type {
		case EventTypeValveTransition:
			from, _ := metadata["from"].(string)
			to, _ := metadata["to"].(string)
			_, err := tx.Exec(ctx,
				`INSERT INTO pipeline_events (pipeline_id, event_type, old_state, new_state, metadata)
				 VALUES ($1, $2, $3, $4, $5)`,
				pid, evt.Type, from, to, metadata,
			)
			if err != nil {
				return err
			}
		case EventTypeBackpressure:
			action, _ := metadata["action"].(string)
			sanctLen, _ := metadata["len"].(int)
			sanctCap, _ := metadata["cap"].(int)
			_, err := tx.Exec(ctx,
				`INSERT INTO backpressure_events (pipeline_id, action, sanctuary_len, sanctuary_cap)
				 VALUES ($1, $2, $3, $4)`,
				pid, action, sanctLen, sanctCap,
			)
			if err != nil {
				return err
			}
		default:
			_, err := tx.Exec(ctx,
				`INSERT INTO pipeline_events (pipeline_id, event_type, metadata)
				 VALUES ($1, $2, $3)`,
				pid, evt.Type, metadata,
			)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

func (r *pgxRecorder) runMigrations(ctx context.Context) error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS pipeline_events (
			id BIGSERIAL PRIMARY KEY,
			pipeline_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			old_state TEXT,
			new_state TEXT,
			metadata JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS backpressure_events (
			id BIGSERIAL PRIMARY KEY,
			pipeline_id TEXT NOT NULL,
			action TEXT NOT NULL,
			sanctuary_len INT NOT NULL,
			sanctuary_cap INT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS disruption_sessions (
			id BIGSERIAL PRIMARY KEY,
			pipeline_id TEXT NOT NULL,
			source_addr TEXT,
			target_addr TEXT,
			held_at TIMESTAMPTZ NOT NULL,
			drained_at TIMESTAMPTZ,
			duration_us BIGINT,
			backpressure_triggered BOOLEAN NOT NULL DEFAULT FALSE
		)`,
	}

	for _, m := range migrations {
		if _, err := r.pool.Exec(ctx, m); err != nil {
			return err
		}
	}
	return nil
}
