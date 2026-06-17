package valve

import (
	"sync"
	"testing"
)

func TestValveStateString(t *testing.T) {
	tests := []struct {
		state ValveState
		want  string
	}{
		{OPEN, "OPEN"},
		{HELD, "HELD"},
		{DRAINING, "DRAINING"},
		{ValveState(99), "UNKNOWN(99)"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("ValveState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestValveTransitionMatrix(t *testing.T) {
	tests := []struct {
		from  ValveState
		to    ValveState
		valid bool
	}{
		{OPEN, OPEN, false},
		{OPEN, HELD, true},
		{OPEN, DRAINING, false},
		{HELD, OPEN, false},
		{HELD, HELD, false},
		{HELD, DRAINING, true},
		{DRAINING, OPEN, true},
		{DRAINING, HELD, true},
		{DRAINING, DRAINING, false},
	}
	for _, tt := range tests {
		v := NewValve(tt.from)
		err := v.TransitionTo(tt.to)
		if tt.valid && err != nil {
			t.Errorf("%s -> %s: expected valid, got error: %v", tt.from, tt.to, err)
		}
		if !tt.valid && err == nil {
			t.Errorf("%s -> %s: expected invalid, got nil", tt.from, tt.to)
		}
	}
}

func TestValveFullCycle(t *testing.T) {
	v := NewValve(OPEN)

	if v.State() != OPEN {
		t.Fatalf("expected OPEN, got %s", v.State())
	}
	if err := v.TransitionTo(HELD); err != nil {
		t.Fatal(err)
	}
	if v.State() != HELD {
		t.Fatalf("expected HELD, got %s", v.State())
	}
	if err := v.TransitionTo(DRAINING); err != nil {
		t.Fatal(err)
	}
	if v.State() != DRAINING {
		t.Fatalf("expected DRAINING, got %s", v.State())
	}
	if err := v.TransitionTo(OPEN); err != nil {
		t.Fatal(err)
	}
	if v.State() != OPEN {
		t.Fatalf("expected OPEN, got %s", v.State())
	}
}

func TestValveConcurrentCAS(t *testing.T) {
	v := NewValve(OPEN)
	var wg sync.WaitGroup
	success := make(chan struct{}, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.TransitionTo(HELD); err == nil {
				success <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(success)

	count := 0
	for range success {
		count++
	}
	if count != 1 {
		t.Errorf("expected exactly 1 successful CAS, got %d", count)
	}
}
