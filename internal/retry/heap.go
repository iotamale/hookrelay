package retry

import "time"

type Job struct {
	Payload       any
	Attempt       int
	NextAttemptAt time.Time
}

type jobHeap []Job

func (h jobHeap) Len() int {
	return len(h)
}

func (h jobHeap) Less(i, j int) bool {
	return h[i].NextAttemptAt.Before(h[j].NextAttemptAt)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *jobHeap) Push(x any) {
	*h = append(*h, x.(Job))
}

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
