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
				ID:        fmt.Sprintf("sub-%d", id),
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

	b := NewBroker(10, 2)
	b.Start()

	b.Subscribe("order.paid", Subscriber{
		ID:        "sub-test",
		TargetURL: ts.URL,
	})

	b.Publish(Event{
		ID:        "evt-123",
		Topic:     "order.paid",
		Payload:   json.RawMessage(`{"amount": 99.99}`),
		Timestamp: time.Now(),
	})

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

	b := NewBroker(10, 2)
	b.Start()

	b.Subscribe("security.events", Subscriber{
		ID:        "sub-secure",
		TargetURL: ts.URL,
		Secret:    webhookSecret,
	})

	b.Publish(Event{
		ID:        "evt-sec-1",
		Topic:     "security.events",
		Payload:   json.RawMessage(`{"status":"compromised"}`),
		Timestamp: time.Now().UTC(),
	})

	b.Stop()

	if !signatureVerified.Load() {
		t.Fatal("HMAC signature couldn'y be properly verified by the receiver")
	}
}

func TestBroker_RetryLogic(t *testing.T) {
	originalBase := BaseBackoff
	originalMax := MaxBackoff
	BaseBackoff = 10 * time.Millisecond
	MaxBackoff = 50 * time.Millisecond
	defer func() {
		BaseBackoff = originalBase
		MaxBackoff = originalMax
	}()

	var attempts atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := attempts.Add(1)

		if current <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := NewBroker(10, 2)
	b.Start()

	b.Subscribe("orders", Subscriber{
		ID:        "sub",
		TargetURL: ts.URL,
	})

	b.Publish(Event{
		ID:        "evt-retry",
		Topic:     "orders",
		Payload:   json.RawMessage(`{}`),
		Timestamp: time.Now(),
	})

	time.Sleep(150 * time.Millisecond)

	b.Stop()

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attemps (2 failed + 1 succesful), got: %d", got)
	}
}

func TestBroker_CircuitBreaker_FastFailure(t *testing.T) {
	origThreshold := FailuresThreshold
	origTimeout := OpenStateTimeoutDuration
	origBase := BaseBackoff
	origMax := MaxBackoff
	FailuresThreshold = 2
	OpenStateTimeoutDuration = 200 * time.Millisecond
	BaseBackoff = 5 * time.Millisecond
	MaxBackoff = 20 * time.Millisecond
	defer func() {
		FailuresThreshold = origThreshold
		OpenStateTimeoutDuration = origTimeout
		BaseBackoff = origBase
		MaxBackoff = origMax
	}()

	var downstreamHits atomic.Int32

	// Server simulation HTTP 500
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	b := NewBroker(50, 4)
	b.Start()

	b.Subscribe("incident", Subscriber{
		ID:        "sub-dead",
		TargetURL: ts.URL,
	})

	for i := 0; i < 10; i++ {
		b.Publish(Event{
			ID:        fmt.Sprintf("evt-cb-%d", i),
			Topic:     "incident",
			Payload:   json.RawMessage(`{}`),
			Timestamp: time.Now(),
		})
	}

	time.Sleep(50 * time.Millisecond)

	cb := b.getBreaker(ts.URL)
	cb.mu.Lock()
	state := cb.state
	cb.mu.Unlock()

	if state != StateOpen {
		t.Fatalf("expected Circuit Breaker to be OPEN, got state %v", state)
	}

	hits := downstreamHits.Load()
	if hits >= 10 {
		t.Fatalf("Circuit Breaker failed to prevent network calls: received %d downstream hits", hits)
	}

	b.Stop()
}

// Test GC logic
func TestBroker_CircuitBreakerGC(t *testing.T) {
	// We overwrite TTL for testing reasons
	origTTL := BreakerGCTTL
	BreakerGCTTL = 100 * time.Millisecond
	defer func() { BreakerGCTTL = origTTL }()

	b := NewBroker(10, 2)
	now := time.Now()

	b.breakers["url-stale-closed"] = &CircuitBreaker{
		state:        StateClosed,
		lastAccessed: now.Add(-200 * time.Millisecond), // Older than TTL -> to be deleted
	}

	b.breakers["url-fresh-closed"] = &CircuitBreaker{
		state:        StateClosed,
		lastAccessed: now, // Fresh -> stays
	}

	b.breakers["url-stale-open"] = &CircuitBreaker{
		state:        StateOpen,
		lastAccessed: now.Add(-200 * time.Millisecond), // Older, but OPEN -> stays
	}

	b.breakers["url-stale-halfopen"] = &CircuitBreaker{
		state:        StateHalfOpen,
		lastAccessed: now.Add(-200 * time.Millisecond), // Older, but HALOFEN -> stays
	}

	b.cleanUpBreakers()

	if _, exists := b.breakers["url-stale-closed"]; exists {
		t.Errorf("expected url-stale-closed to be deleted")
	}

	if _, exists := b.breakers["url-fresh-closed"]; !exists {
		t.Errorf("expected url-fresh-closed to be kept (was fresh)")
	}

	if _, exists := b.breakers["url-stale-open"]; !exists {
		t.Errorf("expected url-stale-open to be kept (was not closed)")
	}

	if _, exists := b.breakers["url-stale-halfopen"]; !exists {
		t.Errorf("expected url-stale-halfopen to be kept (was not closed)")
	}
}
