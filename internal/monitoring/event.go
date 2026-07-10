package monitoring

import "time"

type ValveTransitionEvent struct {
	PipelineID string
	FromState  string
	ToState    string
	Timestamp  time.Time
}

type BackpressureEvent struct {
	PipelineID   string
	Action       string
	SanctuaryLen int
	SanctuaryCap int
	Timestamp    time.Time
}

type TargetFailureEvent struct {
	PipelineID string
	Error      string
	Timestamp  time.Time
}

type SourceDisconnectEvent struct {
	PipelineID string
	Reason     string
	Timestamp  time.Time
}

type Event struct {
	Type    string
	Payload any
}

const (
	EventTypeValveTransition  = "valve_transition"
	EventTypeBackpressure     = "backpressure"
	EventTypeTargetFailure    = "target_failure"
	EventTypeSourceDisconnect = "source_disconnect"
)
