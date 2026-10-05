package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func baseTestConfig(queueSize, workerCount int) Config {
	return Config{
		QueueSize:                queueSize,
		WorkerCount:              workerCount,
		MaxRetries:               3,
		BaseBackoff:              5 * time.Millisecond,
		MaxBackoff:               50 * time.Millisecond,
		BreakerGCInterval:        1 * time.Hour,
		BreakerGCTTL:             24 * time.Hour,
		FailuresThreshold:        5,
		OpenStateTimeoutDuration: 60 * time.Second,
	}
}

func TestBroker_ConcurrentSubAndGetSubs(t *testing.T) {
	b := NewBroker(baseTestConfig(100, 4))
	const topic = "chat"
	const numGoroutines = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			b.Subscribe(topic, Subscriber{
				ID:        fmt.Sprintf("sub-%d", id),
				TargetURL: fmt.Sprintf("http://localhost:9000/webhook/%d", id),
			})
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

	if finalSubs := b.GetSubscribers(topic); len(finalSubs) != numGoroutines {
		t.Fatalf("expected %d subsribers, got %d", numGoroutines, len(finalSubs))
	}
}

func TestBroker_PublishAndStop(t *testing.T) {
	b := NewBroker(baseTestConfig(100, 4))
	b.Start()

	const topic = "chat"
	for i := 0; i < 5; i++ {
		b.Subscribe(topic, Subscriber{
			ID:        fmt.Sprintf("sub-%d", i),
			TargetURL: "http://localhost:9000/webhook",
		})
	}

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
	b := NewBroker(baseTestConfig(3, 2))

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
		t.Fatalf("buffer cap is 3, so we expected 3 jobs to be queued (load shedding), got %d", queued)
	}
}

func TestBroker_HTTPDelivery(t *testing.T) {
	var receivedEvents atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, received %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("missing header Content-Type: application/json")
		}
		if r.Header.Get("X-Hook-Topic") != "order.paid" {
			t.Errorf("wrong header X-Hook-Topic: %s", r.Header.Get("X-Hook-Topic"))
		}

		var evt Event
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			t.Errorf("error while parsing JSON by receiving client: %v", err)
		}
		if evt.ID != "evt-123" {
			t.Errorf("expected ID evt-123, got %s", evt.ID)
		}

		receivedEvents.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := NewBroker(baseTestConfig(10, 2))
	b.Start()

	b.Subscribe("order.paid", Subscriber{ID: "sub-test", TargetURL: ts.URL})
	b.Publish(Event{ID: "evt-123", Topic: "order.paid", Payload: json.RawMessage(`{}`), Timestamp: time.Now()})
	b.Stop()

	if got := receivedEvents.Load(); got != 1 {
		t.Fatalf("Target server should receive exactly 1 event, received: %d", got)
	}
}

func TestBroker_HMACSignature(t *testing.T) {
	const webhookSecret = "super-secret-key-123"
	var signatureVerified atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sigHeader := r.Header.Get("X-Hook-Signature-256")

		if sigHeader == "" {
			t.Errorf("expected X-Hook-Signature-256, but it was empty")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var rawBody json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&rawBody); err != nil {
			t.Errorf("error parsing body: %v", err)
			return
		}

		expectedSig := SignPayload(rawBody, webhookSecret)
		if sigHeader != expectedSig {
			t.Errorf("wrong HMAC signature!\nreceived:  %s\nexpected: %s", sigHeader, expectedSig)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		signatureVerified.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := NewBroker(baseTestConfig(10, 2))
	b.Start()

	b.Subscribe("sec.events", Subscriber{ID: "s1", TargetURL: ts.URL, Secret: webhookSecret})
	b.Publish(Event{ID: "e1", Topic: "sec.events", Payload: json.RawMessage(`{}`)})
	b.Stop()

	if !signatureVerified.Load() {
		t.Fatal("HMAC signature couldn't be properly verified")
	}
}

func TestBroker_RetryLogic(t *testing.T) {
	var attempts atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := baseTestConfig(10, 2)
	cfg.BaseBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 50 * time.Millisecond

	b := NewBroker(cfg)
	b.Start()

	b.Subscribe("orders", Subscriber{ID: "sub", TargetURL: ts.URL})
	b.Publish(Event{ID: "evt-retry", Topic: "orders", Payload: json.RawMessage(`{}`)})

	time.Sleep(150 * time.Millisecond)
	b.Stop()

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts (2 failed + 1 successful), got: %d", got)
	}
}

func TestBroker_CircuitBreaker_FastFailure(t *testing.T) {
	var downstreamHits atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	cfg := baseTestConfig(50, 4)
	cfg.FailuresThreshold = 2
	cfg.OpenStateTimeoutDuration = 200 * time.Millisecond
	b := NewBroker(cfg)
	b.Start()

	b.Subscribe("incident", Subscriber{ID: "sub-dead", TargetURL: ts.URL})

	for i := 0; i < 10; i++ {
		b.Publish(Event{ID: fmt.Sprintf("evt-cb-%d", i), Topic: "incident", Payload: json.RawMessage(`{}`)})
	}

	time.Sleep(50 * time.Millisecond)

	cb := b.getBreaker(ts.URL)
	cb.mu.Lock()
	state := cb.state
	cb.mu.Unlock()

	if state != StateOpen {
		t.Fatalf("expected Circuit Breaker to be OPEN, got state %v", state)
	}

	if hits := downstreamHits.Load(); hits >= 10 {
		t.Fatalf("Circuit Breaker failed to prevent network calls: received %d downstream hits", hits)
	}
	b.Stop()
}

// Test GC logic
func TestBroker_CircuitBreakerGC(t *testing.T) {
	cfg := baseTestConfig(10, 2)
	cfg.BreakerGCTTL = 100 * time.Millisecond
	b := NewBroker(cfg)

	now := time.Now()
	staleTime := now.Add(-200 * time.Millisecond)

	b.breakers["url-stale-closed"] = &CircuitBreaker{
		state: StateClosed, lastAccessed: staleTime, failuresThreshold: cfg.FailuresThreshold, openStateTimeoutDuration: cfg.OpenStateTimeoutDuration,
	}
	b.breakers["url-fresh-closed"] = &CircuitBreaker{
		state: StateClosed, lastAccessed: now, failuresThreshold: cfg.FailuresThreshold, openStateTimeoutDuration: cfg.OpenStateTimeoutDuration,
	}
	b.breakers["url-stale-open"] = &CircuitBreaker{
		state: StateOpen, lastAccessed: staleTime, failuresThreshold: cfg.FailuresThreshold, openStateTimeoutDuration: cfg.OpenStateTimeoutDuration,
	}

	b.cleanUpBreakers()

	if _, exists := b.breakers["url-stale-closed"]; exists {
		t.Errorf("expected url-stale-closed to be deleted")
	}
	if _, exists := b.breakers["url-fresh-closed"]; !exists {
		t.Errorf("expected url-fresh-closed to be kept")
	}
	if _, exists := b.breakers["url-stale-open"]; !exists {
		t.Errorf("expected url-stale-open to be kept")
	}
}

func TestBroker_UnsubscribeByUrl(t *testing.T) {
	b := NewBroker(baseTestConfig(10, 2))
	topic := "alerts"

	b.Subscribe(topic, Subscriber{ID: "sub-1", TargetURL: "http://url1.com"})
	b.Subscribe(topic, Subscriber{ID: "sub-2", TargetURL: "http://url2.com"})
	b.Subscribe(topic, Subscriber{ID: "sub-3", TargetURL: "http://url1.com"})

	removed := b.UnsubscribeByUrl(topic, "http://url1.com")
	if !removed {
		t.Fatalf("expected UnsubscribeByUrl to return true")
	}

	subs := b.GetSubscribers(topic)
	if len(subs) != 1 || subs[0].TargetURL != "http://url2.com" {
		t.Fatalf("expected 1 remaining subscriber with url2, got: %+v", subs)
	}

	removed = b.UnsubscribeByUrl(topic, "http://nonexistent.com")
	if removed {
		t.Fatalf("expected UnsubscribeByUrl to return false for non-existent url")
	}
}

func TestBroker_IsRunning(t *testing.T) {
	b := NewBroker(baseTestConfig(10, 2))

	if b.IsRunning() {
		t.Fatalf("broker should not be running before Start()")
	}

	b.Start()
	if !b.IsRunning() {
		t.Fatalf("broker should be running after Start()")
	}

	b.Stop()
	if b.IsRunning() {
		t.Fatalf("broker should not be running after Stop()")
	}
}

func TestBroker_GetStats(t *testing.T) {
	b := NewBroker(baseTestConfig(10, 2))

	b.successCount.Store(10)
	b.FailedAttempts.Store(3)
	b.droppedCount.Store(1)

	stats := b.GetStats()
	if stats.SuccessfulDeliveries != 10 {
		t.Errorf("expected 10 successful deliveries, got %d", stats.SuccessfulDeliveries)
	}
	if stats.DroppedDeliveries != 1 {
		t.Errorf("expected 1 dropped delivery, got %d", stats.DroppedDeliveries)
	}
	if stats.FailedAttempts != 3 {
		t.Errorf("expected 3 failed attempts, got %d", stats.FailedAttempts)
	}
	if stats.JobQueueSize != 0 {
		t.Errorf("expected 0 queue size, got %d", stats.JobQueueSize)
	}
}
