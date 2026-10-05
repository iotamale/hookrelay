package broker

import (
	"net/http"
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
