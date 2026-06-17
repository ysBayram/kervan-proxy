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
