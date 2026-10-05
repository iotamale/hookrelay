package broker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDelivery_ParseRetryAfter(t *testing.T) {
	// Test seconds format
	delay, err := parseRetryAfter("120")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if delay != 120*time.Second {
		t.Errorf("expected 120s, got %v", delay)
	}

	// Test HTTP-date format (future)
	future := time.Now().UTC().Add(5 * time.Minute).Format(http.TimeFormat)
	delay, err = parseRetryAfter(future)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if delay < 4*time.Minute || delay > 6*time.Minute {
		t.Errorf("expected ~5m, got %v", delay)
	}

	// Test HTTP-date format (past)
	past := time.Now().UTC().Add(-5 * time.Minute).Format(http.TimeFormat)
	delay, err = parseRetryAfter(past)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if delay != 0 {
		t.Errorf("expected 0, got %v", delay)
	}

	// Test invalid format
	_, err = parseRetryAfter("invalid")
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestBroker_RetryAfterHeader_Integration(t *testing.T) {
	var hits int
	var firstHitTime time.Time
	var secondHitTime time.Time

	done := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if hits == 1 {
			firstHitTime = time.Now()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}

		if hits == 2 {
			secondHitTime = time.Now()
			w.WriteHeader(http.StatusOK)
			close(done)
			return
		}
	}))
	defer ts.Close()

	// Use a long base backoff to prove we override it
	cfg := Config{
		QueueSize:                10,
		WorkerCount:              1,
		MaxRetries:               3,
		BaseBackoff:              5 * time.Second,
		MaxBackoff:               10 * time.Second,
		FailuresThreshold:        5,
		OpenStateTimeoutDuration: 10 * time.Second,
		BreakerGCInterval:        10 * time.Second,
		BreakerGCTTL:             10 * time.Second,
	}

	b := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Subscribe("test_topic", Subscriber{ID: "sub-1", TargetURL: ts.URL})

	b.Publish(Event{
		ID:        "evt-1",
		Topic:     "test_topic",
		Payload:   json.RawMessage(`{}`),
		Timestamp: time.Now(),
	})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("test timed out, likely ignored Retry-After and used 5s backoff")
	}

	diff := secondHitTime.Sub(firstHitTime)
	if diff < 1*time.Second {
		t.Errorf("expected at least 1s delay, got %v", diff)
	}
}
