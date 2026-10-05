package breaker

import (
	"testing"
	"time"
)

func TestManager_GC(t *testing.T) {
	m := NewManager(5, time.Minute, time.Second, 100*time.Millisecond)

	now := time.Now()
	staleTime := now.Add(-200 * time.Millisecond)

	m.breakers["url-stale-closed"] = &CircuitBreaker{
		state: StateClosed, lastAccessed: staleTime, failuresThreshold: m.failuresThreshold, openStateTimeoutDuration: m.openStateTimeoutDuration,
	}
	m.breakers["url-fresh-closed"] = &CircuitBreaker{
		state: StateClosed, lastAccessed: now, failuresThreshold: m.failuresThreshold, openStateTimeoutDuration: m.openStateTimeoutDuration,
	}
	m.breakers["url-stale-open"] = &CircuitBreaker{
		state: StateOpen, lastAccessed: staleTime, failuresThreshold: m.failuresThreshold, openStateTimeoutDuration: m.openStateTimeoutDuration,
	}

	m.cleanUp()

	if _, exists := m.breakers["url-stale-closed"]; exists {
		t.Errorf("expected url-stale-closed to be deleted")
	}
	if _, exists := m.breakers["url-fresh-closed"]; !exists {
		t.Errorf("expected url-fresh-closed to be kept")
	}
	if _, exists := m.breakers["url-stale-open"]; !exists {
		t.Errorf("expected url-stale-open to be kept")
	}
}
