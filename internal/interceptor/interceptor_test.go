package interceptor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ysBayram/kervan-proxy/internal/valve"
)

func TestEvaluateFiresOnTruePredicate(t *testing.T) {
	ic := NewInterceptorChain()
	var fired int32
	ic.AddRule(Rule{
		Name:      "always",
		Predicate: func() bool { return true },
		Action:    func() { atomic.AddInt32(&fired, 1) },
	})
	results := ic.Evaluate()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Fired {
		t.Fatal("expected rule to fire")
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatal("expected action to execute once")
	}
}

func TestEvaluateSkipsOnFalsePredicate(t *testing.T) {
	ic := NewInterceptorChain()
	ic.AddRule(Rule{
		Name:      "never",
		Predicate: func() bool { return false },
		Action:    func() { t.Error("action should not fire") },
	})
	results := ic.Evaluate()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Fired {
		t.Fatal("expected rule not to fire")
	}
}

func TestEvaluateEventFiltering(t *testing.T) {
	ic := NewInterceptorChain()
	var valveFired, backFired int32

	ic.AddRule(Rule{
		Name:      "on-valve",
		OnEvent:   EventValveTransition,
		Predicate: func() bool { return true },
		Action:    func() { atomic.AddInt32(&valveFired, 1) },
	})
	ic.AddRule(Rule{
		Name:      "on-backpressure",
		OnEvent:   EventBackpressure,
		Predicate: func() bool { return true },
		Action:    func() { atomic.AddInt32(&backFired, 1) },
	})

	results := ic.EvaluateEvent(Event{Type: EventValveTransition})
	if atomic.LoadInt32(&valveFired) != 1 {
		t.Fatal("expected valve rule to fire")
	}
	if atomic.LoadInt32(&backFired) != 0 {
		t.Fatal("expected backpressure rule NOT to fire")
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if !results[0].Fired || results[1].Fired {
		t.Fatal("unexpected firing pattern")
	}
}

func TestEventAnyMatchesAllEvents(t *testing.T) {
	ic := NewInterceptorChain()
	var fired int32

	ic.AddRule(Rule{
		Name:      "catch-all",
		OnEvent:   EventAny,
		Predicate: func() bool { return true },
		Action:    func() { atomic.AddInt32(&fired, 1) },
	})

	ic.EvaluateEvent(Event{Type: EventValveTransition})
	ic.EvaluateEvent(Event{Type: EventBackpressure})
	ic.EvaluateEvent(Event{Type: EventTargetFailure})
	ic.EvaluateEvent(Event{Type: EventSourceDisconnect})

	if atomic.LoadInt32(&fired) != 4 {
		t.Fatalf("expected 4 firings, got %d", fired)
	}
}

func TestConsecutiveReadErrors(t *testing.T) {
	counter := NewCounter(0)

	pred := ConsecutiveReadErrors(counter, 3)

	if pred() {
		t.Fatal("expected false before threshold")
	}

	counter.Inc()
	counter.Inc()

	if pred() {
		t.Fatal("expected false below threshold")
	}

	counter.Inc()

	if !pred() {
		t.Fatal("expected true at threshold")
	}

	counter.Inc()

	if !pred() {
		t.Fatal("expected true above threshold")
	}

	counter.Reset()

	if pred() {
		t.Fatal("expected false after reset")
	}
}

func TestLatencyAbove(t *testing.T) {
	SetMeasuredLatency(50 * time.Millisecond)
	pred := LatencyAbove(100 * time.Millisecond)

	if pred() {
		t.Fatal("expected false below threshold")
	}

	SetMeasuredLatency(150 * time.Millisecond)

	if !pred() {
		t.Fatal("expected true above threshold")
	}
}

func TestSanctuaryUsageAbove(t *testing.T) {
	s := &mockSanctuary{capacity: 100}

	pred := SanctuaryUsageAbove(s, 0.8)

	s.length = 50
	if pred() {
		t.Fatal("expected false when usage below threshold")
	}

	s.length = 80
	if !pred() {
		t.Fatal("expected true when usage at threshold")
	}

	s.length = 95
	if !pred() {
		t.Fatal("expected true when usage above threshold")
	}
}

func TestSanctuaryUsageAboveZeroCapacity(t *testing.T) {
	s := &mockSanctuary{capacity: 0, length: 10}
	pred := SanctuaryUsageAbove(s, 0.5)

	if pred() {
		t.Fatal("expected false when capacity is zero")
	}
}

func TestTransitionValveToAction(t *testing.T) {
	v := &mockValve{state: valve.OPEN}

	action := TransitionValveTo(v, "HELD")
	action()

	if v.state != valve.HELD {
		t.Fatalf("expected HELD, got %s", v.state)
	}
}

func TestApplyBackpressureDropOldest(t *testing.T) {
	s := &mockSanctuary{capacity: 10, length: 10}

	action := ApplyBackpressureAction(s, "drop_oldest")
	action()

	if s.dropped != 1 {
		t.Fatalf("expected 1 drop, got %d", s.dropped)
	}
}

func TestInterceptorChainRulesOrder(t *testing.T) {
	ic := NewInterceptorChain()
	var order []string

	ic.AddRule(Rule{
		Name:      "first",
		Predicate: func() bool { return true },
		Action:    func() { order = append(order, "first") },
	})
	ic.AddRule(Rule{
		Name:      "second",
		Predicate: func() bool { return true },
		Action:    func() { order = append(order, "second") },
	})
	ic.AddRule(Rule{
		Name:      "third",
		Predicate: func() bool { return true },
		Action:    func() { order = append(order, "third") },
	})

	ic.Evaluate()

	if len(order) != 3 || order[0] != "first" || order[1] != "second" || order[2] != "third" {
		t.Fatalf("expected order 'first,second,third', got %v", order)
	}
}

func TestS2ErrorThresholdToHeld(t *testing.T) {
	v := &mockValve{state: valve.OPEN}
	errCounter := NewCounter(0)

	ic := NewInterceptorChain()
	ic.AddRule(Rule{
		Name:      "error-threshold",
		OnEvent:   EventTargetFailure,
		Predicate: ConsecutiveReadErrors(errCounter, 3),
		Action:    TransitionValveTo(v, "HELD"),
	})

	errCounter.Inc()
	errCounter.Inc()

	ic.EvaluateEvent(Event{Type: EventTargetFailure})

	if v.state != valve.OPEN {
		t.Fatal("expected still OPEN below threshold")
	}

	errCounter.Inc()

	ic.EvaluateEvent(Event{Type: EventTargetFailure})

	if v.state != valve.HELD {
		t.Fatal("expected HELD after threshold reached")
	}
}

func TestS2HeldToDrainingToOpen(t *testing.T) {
	v := &mockValve{state: valve.HELD}

	ic := NewInterceptorChain()
	var healthChecks int32
	ic.AddRule(Rule{
		Name: "target-healthy",
		Predicate: func() bool {
			return atomic.LoadInt32(&healthChecks) >= 2
		},
		Action: TransitionValveTo(v, "DRAINING"),
	})

	atomic.StoreInt32(&healthChecks, 1)
	ic.Evaluate()
	if v.state != valve.HELD {
		t.Fatal("expected still HELD before 2 health checks")
	}

	atomic.StoreInt32(&healthChecks, 2)
	ic.Evaluate()
	if v.state != valve.DRAINING {
		t.Fatal("expected DRAINING after healthy checks")
	}

	clearS2 := false
	drainIC := NewInterceptorChain()
	drainIC.AddRule(Rule{
		Name: "drain-complete",
		Predicate: func() bool {
			return clearS2
		},
		Action: TransitionValveTo(v, "OPEN"),
	})

	clearS2 = true
	drainIC.Evaluate()
	if v.state != valve.OPEN {
		t.Fatal("expected OPEN after drain complete")
	}
}

func TestS3BackpressureOnUsageThreshold(t *testing.T) {
	s := &mockSanctuary{capacity: 100, length: 90}

	ic := NewInterceptorChain()
	ic.AddRule(Rule{
		Name:      "backpressure-drop-oldest",
		OnEvent:   EventBackpressure,
		Predicate: SanctuaryUsageAbove(s, 0.85),
		Action:    ApplyBackpressureAction(s, "drop_oldest"),
	})

	ic.EvaluateEvent(Event{Type: EventBackpressure})

	if s.dropped != 1 {
		t.Fatalf("expected 1 dropped item, got %d", s.dropped)
	}

	s.length = 50
	s.dropped = 0

	ic.EvaluateEvent(Event{Type: EventBackpressure})

	if s.dropped != 0 {
		t.Fatal("expected no drops when usage below threshold")
	}
}

type mockValve struct {
	mu    sync.Mutex
	state valve.ValveState
}

func (v *mockValve) State() valve.ValveState {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.state
}

func (v *mockValve) TransitionTo(target valve.ValveState) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.state = target
	return nil
}

type mockSanctuary struct {
	capacity int
	length   int
	dropped  int
}

func (s *mockSanctuary) Len() int { return s.length }
func (s *mockSanctuary) Cap() int { return s.capacity }
func (s *mockSanctuary) DropOldest() bool {
	s.dropped++
	s.length--
	return true
}
