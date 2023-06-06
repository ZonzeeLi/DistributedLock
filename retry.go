package DistributedLock

import "time"

type RetryStrategy interface {
	// Next returns the next retry delay. If there are no more retries, it returns false.
	Next() (time.Duration, bool)
}

type FixedRetryStrategy struct {
	Intervals time.Duration
	Max       int
	cnt       int
}

func (f *FixedRetryStrategy) Next() (time.Duration, bool) {
	f.cnt++
	return f.Intervals, f.cnt <= f.Max
}
