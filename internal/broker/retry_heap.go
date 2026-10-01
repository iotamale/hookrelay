package broker

import (
	"math/rand"
	"time"
)

// retryHeap implements container/heap interface for DeliveryJob slice.
// It acts as a Min-Heap prioritizing jobs with the earliest NextAttemptAt time.
type retryHeap []DeliveryJob

func (h retryHeap) Len() int {
	return len(h)
}

func (h retryHeap) Less(i, j int) bool {
	return h[i].NextAttemptAt.Before(h[j].NextAttemptAt)
}

func (h retryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *retryHeap) Push(x any) {
	job := x.(DeliveryJob)
	*h = append(*h, job)
}

func (h *retryHeap) Pop() any {
	old := *h
	n := len(old)
	job := old[n-1]
	*h = old[0 : n-1]
	return job
}

// base * 2^attempt + jitter
func nextBackoff(attempt int, base, max time.Duration) time.Duration {
	shift := 1 << attempt
	delay := base * time.Duration(shift)

	if delay > max || delay <= 0 {
		delay = max
	}

	// random val from [0, delay)
	jitter := time.Duration(rand.Int63n(int64(delay)))

	return jitter
}
