package interceptor

type ValveController interface {
	State() string
	TransitionTo(target string) error
}

type BackpressureController interface {
	Len() int
	Cap() int
	DropOldest() ([]byte, bool)
}

func TransitionValveTo(valve ValveController, target string) func() {
	return func() {
		_ = valve.TransitionTo(target)
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
