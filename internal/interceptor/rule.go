package interceptor

type EventType int

const (
	EventAny EventType = iota
	EventValveTransition
	EventBackpressure
	EventTargetFailure
	EventSourceDisconnect
)

type Event struct {
	Type     EventType
	Metadata map[string]any
}

type Rule struct {
	Name      string
	OnEvent   EventType
	Predicate func() bool
	Action    func()
}

func NewRule(name string, predicate func() bool, action func()) Rule {
	return Rule{
		Name:      name,
		Predicate: predicate,
		Action:    action,
	}
}
