package valve

import (
	"errors"
	"sync/atomic"
)

var (
	ErrInvalidTransition = errors.New("valve: invalid state transition")
)

type Valve struct {
	state int32
}

func NewValve(initial ValveState) *Valve {
	return &Valve{state: int32(initial)}
}

func (v *Valve) State() ValveState {
	return ValveState(atomic.LoadInt32(&v.state))
}

func (v *Valve) TransitionTo(target ValveState) error {
	current := v.State()
	if !current.CanTransitionTo(target) {
		return ErrInvalidTransition
	}
	if !atomic.CompareAndSwapInt32(&v.state, int32(current), int32(target)) {
		return ErrInvalidTransition
	}
	return nil
}

func (v *Valve) CanTransitionTo(target ValveState) bool {
	return v.State().CanTransitionTo(target)
}
