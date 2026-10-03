package broker

import (
	"testing"
	"time"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	cb := &CircuitBreaker{
		state:                    StateClosed,
		failuresThreshold:        3,
		openStateTimeoutDuration: 50 * time.Millisecond,
	}

	if !cb.Allow() {
		t.Fatalf("expected circuit breaker to allow requests initially")
	}

	// Record failures up to the threshold
	for i := uint16(0); i < 2; i++ {
		cb.RecordFailure()
	}
	if !cb.Allow() {
		t.Fatalf("expected circuit breaker to remain CLOSED at 1 below the threshold")
	}

	// 3rd failure trips the breaker
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatalf("expected circuit breaker to deny requests when OPEN")
	}

	time.Sleep(60 * time.Millisecond)

	// First request after timeout should be allowed
	if !cb.Allow() {
		t.Fatalf("expected circuit breaker to allow exactly one probe request after timeout")
	}

	// Immediate subsequent requests should be denied
	if cb.Allow() {
		t.Fatalf("expected circuit breaker to deny concurrent requests while in HALF-OPEN state")
	}

	// Probe request succeeds
	cb.RecordSuccess()

	// Circuit should be fully CLOSED again
	if !cb.Allow() {
		t.Fatalf("expected circuit breaker to allow requests after recovering to CLOSED state")
	}
}
