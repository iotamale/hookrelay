package broker

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

type Subscriber struct {
	ID string
	TargetURL string
}

type Event struct {
	ID string
	Topic string
	Payload json.RawMessage
	Timestamp time.Time
}

type Broker struct {
	mu sync.RWMutex
	subscribers map[string][]Subscriber		// topic -> []Subscribers
	jobs chan DeliveryJob
	wg sync.WaitGroup
	workerCount int
	processed atomic.Int64
}

type DeliveryJob struct {
	Subscriber Subscriber
	Event Event
}

func NewBroker(queueSize int, workerCount int) *Broker {
	return &Broker{
		subscribers: make(map[string][]Subscriber),
		jobs: make(chan DeliveryJob, queueSize),
		workerCount: workerCount,
	}
}

func (b *Broker) Subscribe(topic string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscribers[topic] = append(b.subscribers[topic], sub)
}

func (b *Broker) GetSubscribers(topic string) []Subscriber {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs := b.subscribers[topic]
	targets := make([]Subscriber, len(subs))
	copy(targets, subs)

	return targets
}

func (b *Broker) Start() {
	for i := 0; i < b.workerCount; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}
}

func (b *Broker) worker(id int) {
	defer b.wg.Done()

	for job := range b.jobs {
		_ = job
		_ = id
		b.processed.Add(1)
	}

}

func (b *Broker) Publish(evt Event) int {
	subs := b.GetSubscribers(evt.Topic)
	queued := 0

	for _, sub := range subs {
		deliveryJob := DeliveryJob{
			Subscriber: sub,
			Event: evt,
		}

		select {
		case b.jobs <- deliveryJob:
			queued++
		default:
			// buffer is full
		}
	}

	return queued
}

func (b *Broker) Stop() {
	close(b.jobs)

	b.wg.Wait()

}

func (b *Broker) ProcessedCount() int64 {
	return b.processed.Load()
}