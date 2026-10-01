package broker

import (
	"container/heap"
	"testing"
	"time"
)

func TestRetryHeap_Ordering(t *testing.T) {
	h := &retryHeap{}
	heap.Init(h)

	now := time.Now()

	heap.Push(h, DeliveryJob{Event: Event{ID: "job-late"}, NextAttemptAt: now.Add(10 * time.Second)})
	heap.Push(h, DeliveryJob{Event: Event{ID: "job-soon"}, NextAttemptAt: now.Add(1 * time.Second)})
	heap.Push(h, DeliveryJob{Event: Event{ID: "job-now"}, NextAttemptAt: now})

	expectedOrder := []string{"job-now", "job-soon", "job-late"}

	for _, expectedID := range expectedOrder {
		popped := heap.Pop(h).(DeliveryJob)
		if popped.Event.ID != expectedID {
			t.Errorf("expected job %s, got %s", expectedID, popped.Event.ID)
		}
	}
}

func TestNextBackoff_Bounds(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	delay1 := nextBackoff(1, base, max)
	if delay1 < 0 || delay1 >= 2*time.Second {
		t.Errorf("wrong backoff for attempt #1: %v", delay1)
	}

	delay10 := nextBackoff(10, base, max)
	if delay10 < 0 || delay10 >= max {
		t.Errorf("wrong backoff for attempt #10, limit didn't work: %v", delay10)
	}
}
