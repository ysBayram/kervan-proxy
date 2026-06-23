//go:build !monitoring

package monitoring

import "context"

type noopRecorder struct{}

func NewRecorder(_ *EventBus, _ string) Recorder {
	return &noopRecorder{}
}

func (r *noopRecorder) Start(_ context.Context) error { return nil }

func (r *noopRecorder) Stop() error { return nil }
