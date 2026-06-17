package interceptor

import (
	"sync/atomic"
	"time"
)

type Counter struct {
	value int32
}

func NewCounter(initial int32) *Counter {
	return &Counter{value: initial}
}

func (c *Counter) Inc() int32 {
	return atomic.AddInt32(&c.value, 1)
}

func (c *Counter) Reset() {
	atomic.StoreInt32(&c.value, 0)
}

func (c *Counter) Value() int32 {
	return atomic.LoadInt32(&c.value)
}

func ConsecutiveReadErrors(counter *Counter, threshold int32) func() bool {
	return func() bool {
		return counter.Value() >= threshold
	}
}

func LatencyAbove(threshold time.Duration) func() bool {
	return func() bool {
		return time.Duration(atomic.LoadInt64(&measuredLatency)) > threshold
	}
}

var measuredLatency int64

func SetMeasuredLatency(d time.Duration) {
	atomic.StoreInt64(&measuredLatency, int64(d))
}

func CurrentMeasuredLatency() time.Duration {
	return time.Duration(atomic.LoadInt64(&measuredLatency))
}

type UsageProvider interface {
	Len() int
	Cap() int
}

func SanctuaryUsageAbove(sanctuary UsageProvider, threshold float64) func() bool {
	return func() bool {
		if sanctuary.Cap() == 0 {
			return false
		}
		usage := float64(sanctuary.Len()) / float64(sanctuary.Cap())
		return usage >= threshold
	}
}
