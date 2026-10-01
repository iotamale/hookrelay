package broker

import (
	"bytes"
	"container/heap"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	MaxRetries  = 5
	BaseBackoff = 1 * time.Second
	MaxBackoff  = 1 * time.Hour
)

type Subscriber struct {
	ID        string `json:"id"`
	TargetURL string `json:"target_url"`
	Secret    string `json:"secret,omitempty"`
}

type Event struct {
	ID        string          `json:"id"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

type Broker struct {
	mu          sync.RWMutex
	subscribers map[string][]Subscriber // topic -> []Subscribers
	jobs        chan DeliveryJob
	wg          sync.WaitGroup // tracks active worker goroutines
	schedulerWg sync.WaitGroup // tracks retry scheduler goroutine
	workerCount int
	processed   atomic.Int64
	client      *http.Client
	retryMu     sync.Mutex    // protects retires min heap
	retries     *retryHeap    // Min-Heap ordering failed jobs by Broker.NextAttemptAt
	newRetry    chan struct{} // wakes up retry scheduler when new job is pushed to the heap
	done        chan struct{}
}

type DeliveryJob struct {
	Subscriber    Subscriber
	Event         Event
	Attempt       int
	NextAttemptAt time.Time
}

func NewBroker(queueSize int, workerCount int) *Broker {
	h := &retryHeap{}
	heap.Init(h)

	return &Broker{
		subscribers: make(map[string][]Subscriber),
		jobs:        make(chan DeliveryJob, queueSize),
		workerCount: workerCount,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		retries:  h,
		newRetry: make(chan struct{}, 1),
		done:     make(chan struct{}),
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
	b.schedulerWg.Add(1)
	go b.retryScheduler()

	for i := 0; i < b.workerCount; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}
}

func (b *Broker) Stop() {
	// Signal retry scheduler to stop accepting new jobs
	close(b.done)
	b.schedulerWg.Wait()

	// Close the main job queue
	close(b.jobs)
	b.wg.Wait()
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
			Event:      evt,
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

	isTransient := false

	if err != nil {
		// network errors
		isTransient = true
	} else {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}

		if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			isTransient = true
		} else {
			slog.Warn("permanent client error, dropping webhook", "url", job.Subscriber.TargetURL, "status", resp.StatusCode)
			return
		}
	}

	if isTransient {
		if job.Attempt < MaxRetries {
			job.Attempt++
			delay := nextBackoff(job.Attempt, BaseBackoff, MaxBackoff)
			job.NextAttemptAt = time.Now().Add(delay)

			slog.Info("transient error, scheduling retry", "url", job.Subscriber.TargetURL, "attempt", job.Attempt, "delay", delay)
			b.scheduleRetry(job)
		} else {
			slog.Warn("exhausted retries, dropping webhook", "event_id", job.Event.ID, "url", job.Subscriber.TargetURL)
		}
	}
}

func (b *Broker) scheduleRetry(job DeliveryJob) {
	b.retryMu.Lock()
	heap.Push(b.retries, job)
	b.retryMu.Unlock()

	// Wake up the scheduler
	select {
	case b.newRetry <- struct{}{}:
	default:
	}
}

func (b *Broker) retryScheduler() {
	defer b.schedulerWg.Done()

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		b.retryMu.Lock()
		if b.retries.Len() == 0 {
			b.retryMu.Unlock()
			// The heap is empty, sleep until a new retry is scheduled.
			select {
			case <-b.newRetry:
				continue
			case <-b.done:
				return
			}
		}

		nextJob := (*b.retries)[0]
		now := time.Now()

		if now.After(nextJob.NextAttemptAt) || now.Equal(nextJob.NextAttemptAt) {
			job := heap.Pop(b.retries).(DeliveryJob)
			b.retryMu.Unlock()

			select {
			case b.jobs <- job:
			default:
				// buffer is full
			}
			continue
		}

		delay := nextJob.NextAttemptAt.Sub(now)
		b.retryMu.Unlock()

		timer.Reset(delay)

		select {
		case <-timer.C:
		case <-b.newRetry:
			if !timer.Stop() {
				<-timer.C
			}
		case <-b.done:
			timer.Stop()
			return
		}
	}
}

func SignPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)

	hashString := hex.EncodeToString(mac.Sum(nil))
	return "sha256=" + hashString
}
