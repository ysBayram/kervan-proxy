package sanctuary

import "errors"

var ErrBufferFull = errors.New("sanctuary buffer full")

type BackpressureAction int

const (
	DropOldest BackpressureAction = iota
	RejectNew
)

func (a BackpressureAction) String() string {
	switch a {
	case DropOldest:
		return "drop_oldest"
	case RejectNew:
		return "reject_new"
	default:
		return "unknown"
	}
}
