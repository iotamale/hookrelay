package broker

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Subscriber struct {
	ID string
	TargetURL string
	Secret string
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
	client *http.Client
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
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
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
		b.deliver(job)
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

func (b *Broker) deliver(job DeliveryJob) {
	body, err := json.Marshal(job.Event)

	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, job.Subscriber.TargetURL, bytes.NewReader(body))

	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hook-Event-ID", job.Event.ID)
	req.Header.Set("X-Hook-Topic", job.Event.Topic)

	if job.Subscriber.Secret != "" {
		hashString := SignPayload(body, job.Subscriber.Secret)
		req.Header.Set("X-Hook-Signature-256", hashString)
	}

	resp, err := b.client.Do(req)

	if err != nil {
		return
	}

	defer resp.Body.Close()
}

func SignPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)

	hashString := hex.EncodeToString(mac.Sum(nil))
	return "sha256=" + hashString
}