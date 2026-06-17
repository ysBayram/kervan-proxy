package valve

import "fmt"

type ValveState int32

const (
	OPEN     ValveState = 0
	HELD     ValveState = 1
	DRAINING ValveState = 2
)

func (s ValveState) String() string {
	switch s {
	case OPEN:
		return "OPEN"
	case HELD:
		return "HELD"
	case DRAINING:
		return "DRAINING"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(s))
	}
}

var validTransitions = map[ValveState]map[ValveState]bool{
	OPEN:     {HELD: true},
	HELD:     {DRAINING: true},
	DRAINING: {OPEN: true, HELD: true},
}

func (s ValveState) CanTransitionTo(target ValveState) bool {
	transitions, ok := validTransitions[s]
	if !ok {
		return false
	}
	return transitions[target]
}
