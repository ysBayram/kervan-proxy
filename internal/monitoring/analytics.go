//go:build monitoring

package monitoring

import (
	"context"
	"time"
)

type DisruptionSession struct {
	PipelineID           string    `json:"pipeline_id"`
	SourceAddr           string    `json:"source_addr"`
	TargetAddr           string    `json:"target_addr"`
	HeldAt               time.Time `json:"held_at"`
	DrainedAt            *time.Time `json:"drained_at,omitempty"`
	DurationUs           *int64    `json:"duration_us,omitempty"`
	BackpressureTriggered bool      `json:"backpressure_triggered"`
}

type SourceDisruption struct {
	PipelineID    string  `json:"pipeline_id"`
	SourceAddr    string  `json:"source_addr"`
	TotalSessions int64   `json:"total_sessions"`
	AvgDurationUs float64 `json:"avg_duration_us"`
}

type BackpressureFreq struct {
	Action        string  `json:"action"`
	Count         int64   `json:"count"`
	AvgSanctuaryPct float64 `json:"avg_sanctuary_pct"`
}

func (r *pgxRecorder) GetTopDisruptedSources(ctx context.Context, limit int) ([]SourceDisruption, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT pipeline_id, source_addr, COUNT(*) as total_sessions,
			   COALESCE(AVG(duration_us), 0) as avg_duration_us
		FROM disruption_sessions
		GROUP BY pipeline_id, source_addr
		ORDER BY total_sessions DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SourceDisruption
	for rows.Next() {
		var sd SourceDisruption
		if err := rows.Scan(&sd.PipelineID, &sd.SourceAddr, &sd.TotalSessions, &sd.AvgDurationUs); err != nil {
			return nil, err
		}
		results = append(results, sd)
	}
	return results, rows.Err()
}

func (r *pgxRecorder) GetAvgRecoveryTime(ctx context.Context) (float64, error) {
	var avg float64
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(AVG(duration_us), 0) FROM disruption_sessions
		WHERE drained_at IS NOT NULL
	`).Scan(&avg)
	return avg, err
}

func (r *pgxRecorder) GetBackpressureFrequency(ctx context.Context) ([]BackpressureFreq, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT action, COUNT(*) as count,
			   COALESCE(AVG(sanctuary_len::float / NULLIF(sanctuary_cap, 0)), 0) as avg_sanctuary_pct
		FROM backpressure_events
		GROUP BY action
		ORDER BY count DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []BackpressureFreq
	for rows.Next() {
		var bf BackpressureFreq
		if err := rows.Scan(&bf.Action, &bf.Count, &bf.AvgSanctuaryPct); err != nil {
			return nil, err
		}
		results = append(results, bf)
	}
	return results, rows.Err()
}
