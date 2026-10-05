package breaker

import (
	"log/slog"
	"sync"
	"time"
)

type CircuitBreakerState int

const (
	// StateClosed indicates normal operation. All requests are allowed to pass through.
	StateClosed = iota
	// StateOpen indicates that the downstream service is failing. All requests are rejected.
	StateOpen
	// StateHalfOpen indicates that the timeout has expired. Exactly one probe request is allowed
	// through to test if the downstream service has recovered.
	StateHalfOpen
)

// CircuitBreaker prevents starving healthy subscribers when a large batch od dead subscribers
// are in the job queue.
type CircuitBreaker struct {
	state         CircuitBreakerState
	mu            sync.Mutex
	failuresCount uint16
	expiresAt     time.Time
	lastAccessed  time.Time
	targetURL     string

	// FailuresThreshold defines how many consecutive failures are allowed
	// before transitions to the OPEN state.
	failuresThreshold uint16
	// OpenStateTimeoutDuration defines how long the circuit remains OPEN
	// before allowing a probe request (transitioning to HALF-OPEN).
	openStateTimeoutDuration time.Duration
}

// RecordFailure increments the failure counter.
// If the failures exceed the FailuresThreshold, the circuit trips (Closed/Half-Open -> Open).
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failuresCount++

	if cb.failuresCount >= cb.failuresThreshold {
		if cb.state != StateOpen {
			slog.Warn("circuit breaker tripped to OPEN", "target_url", cb.targetURL)
		}
		cb.state = StateOpen
		cb.expiresAt = time.Now().Add(cb.openStateTimeoutDuration)
	}
}

// RecordSuccess transitions the circuit back to normal operation (Half-Open/Closed -> Closed).
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != StateClosed {
		slog.Info("circuit breaker recovered to CLOSED", "target_url", cb.targetURL)
	}
	cb.failuresCount = 0
	cb.state = StateClosed
}

// Allow evaluates the current state and decides whether the request should be executed or not.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastAccessed = time.Now()

	switch cb.state {
	case StateOpen:
		if time.Now().After(cb.expiresAt) {
			// Transit to HALF-OPEN. We allow one probe request to check the service health.
			cb.state = StateHalfOpen
			return true
		}

		// Cooldown still in place.
		return false
	case StateHalfOpen:
		return false
	default:
		return true
	}
}

// State returns current state of the circuit breaker.
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
