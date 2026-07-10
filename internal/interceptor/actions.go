package interceptor

import "github.com/ysBayram/kervan-proxy/internal/valve"

type ValveController interface {
	State() valve.ValveState
	TransitionTo(target valve.ValveState) error
}

type BackpressureController interface {
	Len() int
	Cap() int
	DropOldest() bool
}

func TransitionValveTo(vc ValveController, target string) func() {
	return func() {
		var state valve.ValveState
		switch target {
		case valve.OPEN.String():
			state = valve.OPEN
		case valve.HELD.String():
			state = valve.HELD
		case valve.DRAINING.String():
			state = valve.DRAINING
		default:
			return
		}
		_ = vc.TransitionTo(state)
	}
}

func ApplyBackpressureAction(sanctuary BackpressureController, action string) func() {
	return func() {
		switch action {
		case "drop_oldest":
			sanctuary.DropOldest()
		}
	}
}
