package monitoring

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEventBusPublishSubscribe(t *testing.T) {
	bus := NewEventBus(10)
	var received atomic.Int32

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for evt := range bus.Subscribe() {
			if evt.Type == EventTypeValveTransition {
				received.Add(1)
			}
		}
	}()

	bus.Publish(Event{Type: EventTypeValveTransition})
	bus.Publish(Event{Type: EventTypeBackpressure})
	bus.Publish(Event{Type: EventTypeValveTransition})
	bus.Close()

	wg.Wait()

	if received.Load() != 2 {
		t.Fatalf("expected 2 valve transition events, got %d", received.Load())
	}
}

func TestEventBusNonBlockingDrop(t *testing.T) {
	bus := NewEventBus(2)

	for i := 0; i < 10; i++ {
		bus.Publish(Event{Type: EventTypeValveTransition})
	}

	if bus.Dropped() == 0 {
		t.Fatal("expected dropped events when channel full")
	}
}

func TestEventBusCap(t *testing.T) {
	bus := NewEventBus(100)
	if bus.Cap() != 100 {
		t.Fatalf("expected cap 100, got %d", bus.Cap())
	}
}

func TestMonitorRecord(t *testing.T) {
	bus := NewEventBus(10)
	m := NewMonitor(bus)

	var received atomic.Int32
	go func() {
		for range bus.Subscribe() {
			received.Add(1)
		}
	}()

	m.Record(context.Background(), EventTypeTargetFailure, map[string]any{"error": "test"})
	time.Sleep(50 * time.Millisecond)
	bus.Close()
	time.Sleep(50 * time.Millisecond)

	if received.Load() != 1 {
		t.Fatalf("expected 1 event, got %d", received.Load())
	}
}

func TestMonitorRecordNonBlocking(t *testing.T) {
	bus := NewEventBus(0) // no buffer
	m := NewMonitor(bus)

	m.Record(context.Background(), EventTypeTargetFailure, map[string]any{"error": "test"})

	if bus.Dropped() != 1 {
		t.Fatalf("expected 1 dropped event on unbuffered bus, got %d", bus.Dropped())
	}
}

func TestNoopRecorder(t *testing.T) {
	bus := NewEventBus(10)
	r := NewRecorder(bus)

	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkEventBusPublish(b *testing.B) {
	bus := NewEventBus(10000)
	go func() {
		for range bus.Subscribe() {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(Event{Type: EventTypeValveTransition})
	}
	bus.Close()
}

func BenchmarkMonitorRecord(b *testing.B) {
	bus := NewEventBus(10000)
	m := NewMonitor(bus)
	go func() {
		for range bus.Subscribe() {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Record(context.TODO(), EventTypeTargetFailure, map[string]any{"error": "bench"})
	}
	bus.Close()
}

func TestEventConsts(t *testing.T) {
	tests := []struct {
		name string
		val  string
	}{
		{"valve_transition", EventTypeValveTransition},
		{"backpressure", EventTypeBackpressure},
		{"target_failure", EventTypeTargetFailure},
		{"source_disconnect", EventTypeSourceDisconnect},
	}
	for _, tc := range tests {
		if tc.val != tc.name {
			t.Fatalf("expected %q, got %q", tc.name, tc.val)
		}
	}
}
