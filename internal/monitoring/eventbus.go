package monitoring

import (
	"context"
	"sync"
	"sync/atomic"
)

type EventBus struct {
	ch      chan Event
	dropped atomic.Int64
	queued  atomic.Int64
	cap     int

	closeOnce sync.Once
}

func NewEventBus(capacity int) *EventBus {
	return &EventBus{
		ch:  make(chan Event, capacity),
		cap: capacity,
	}
}

func (eb *EventBus) Publish(event Event) {
	select {
	case eb.ch <- event:
		eb.queued.Add(1)
	default:
		eb.dropped.Add(1)
	}
}

func (eb *EventBus) Subscribe() <-chan Event {
	return eb.ch
}

func (eb *EventBus) Close() {
	eb.closeOnce.Do(func() {
		close(eb.ch)
	})
}

func (eb *EventBus) Dropped() int64 {
	return eb.dropped.Load()
}

func (eb *EventBus) Queued() int64 {
	return eb.queued.Load()
}

func (eb *EventBus) Cap() int {
	return eb.cap
}

type Monitor struct {
	bus *EventBus
}

func NewMonitor(bus *EventBus) *Monitor {
	return &Monitor{
		bus: bus,
	}
}

func (m *Monitor) Record(_ context.Context, eventType string, metadata map[string]any) {
	m.bus.Publish(Event{
		Type:    eventType,
		Payload: metadata,
	})
}

func (m *Monitor) Bus() *EventBus {
	return m.bus
}
