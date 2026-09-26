package broker

import (
	"fmt"
	"sync"
	"testing"
)

func TestBroker_ConcurrentSubAndGetSubs(t *testing.T) {
	b := NewBroker(100, 4)
	const topic = "chat"
	const numGoroutines = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			sub := Subscriber{
				ID: fmt.Sprintf("sub-%d", id),
				TargetURL: fmt.Sprintf("http://localhost:9000/webhook/%d", id),
			}
			b.Subscribe(topic, sub)
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			subs := b.GetSubscribers(topic)
			for _, s := range subs {
				if s.ID == "" || s.TargetURL == "" {
					t.Errorf("retrieved wrong subscriber: %+v", s)
				}
			}
		}()
	}

	wg.Wait()

	finalSubs := b.GetSubscribers(topic)

	if len(finalSubs) != numGoroutines {
		t.Fatalf("expected %d subsribers, got %d", numGoroutines, len(finalSubs))
	}
}

func TestBroker_PublishAndStop(t *testing.T) {
	b := NewBroker(100, 4)
	b.Start()

	const topic = "chat"
	for i := 0; i < 5; i++ {
		b.Subscribe(topic, Subscriber{
			ID:        fmt.Sprintf("sub-%d", i),
			TargetURL: "http://localhost:9000/webhook",
		})
	}

	// publish 10 evenets (for 5 subs ----> 50 events)
	for i := 0; i < 10; i++ {
		queued := b.Publish(Event{
			ID:    fmt.Sprintf("evt-%d", i),
			Topic: topic,
		})
		if queued != 5 {
			t.Fatalf("expected 5 jobs, got %d", queued)
		}
	}

	b.Stop()

	if got := b.ProcessedCount(); got != 50 {
		t.Fatalf("expected 50 fullfilled jobs after b.Stop(), got %d", got)
	}
}

func TestBroker_QueueLoadShedding(t *testing.T) {
	// We create a broker with a small buffer and deliberately don't call
	// b.Start(), which results in buffer being overloaded.
	b := NewBroker(3, 2)

	const topic = "chat"
	for i := 0; i < 5; i++ {
		b.Subscribe(topic, Subscriber{
			ID:        fmt.Sprintf("sub-%d", i),
			TargetURL: "http://localhost:9000/webhook",
		})
	}

	// We publish evenets for 5 subs, but the buffer cap is 3.
	queued := b.Publish(Event{ID: "hello-world", Topic: topic})

	if queued != 3 {
		t.Fatalf("buffer cap is 3, so we expected 3 jobs to be queued (load shedding), got %d jobs queued", queued)
	}
}