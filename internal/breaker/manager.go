package breaker

import (
	"context"
	"sync"
	"time"
)

type Manager struct {
	breakers                 map[string]*CircuitBreaker
	mu                       sync.RWMutex
	failuresThreshold        uint16
	openStateTimeoutDuration time.Duration
	gcInterval               time.Duration
	gcTTL                    time.Duration
}

func NewManager(failuresThreshold uint16, openStateTimeout time.Duration, gcInterval, gcTTL time.Duration) *Manager {
	return &Manager{
		breakers:                 make(map[string]*CircuitBreaker),
		failuresThreshold:        failuresThreshold,
		openStateTimeoutDuration: openStateTimeout,
		gcInterval:               gcInterval,
		gcTTL:                    gcTTL,
	}
}

func (m *Manager) GetBreaker(url string) *CircuitBreaker {
	m.mu.RLock()
	val, exists := m.breakers[url]
	m.mu.RUnlock()

	if exists {
		return val
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	val, exists = m.breakers[url]
	if !exists {
		val = &CircuitBreaker{
			targetURL:                url,
			state:                    StateClosed,
			lastAccessed:             time.Now(),
			failuresThreshold:        m.failuresThreshold,
			openStateTimeoutDuration: m.openStateTimeoutDuration,
		}
		m.breakers[url] = val
	}

	return val
}

func (m *Manager) StartGC(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(m.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanUp()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) cleanUp() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for url, cb := range m.breakers {
		cb.mu.Lock()
		isClosed := cb.state == StateClosed
		isStale := now.Sub(cb.lastAccessed) > m.gcTTL
		cb.mu.Unlock()

		if isClosed && isStale {
			delete(m.breakers, url)
		}
	}
}
