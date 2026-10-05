package retry

import (
	"container/heap"
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type Scheduler struct {
	mu          sync.Mutex
	jobs        *jobHeap
	newJob      chan struct{}
	baseBackoff time.Duration
	maxBackoff  time.Duration
	maxRetries  int
}

func NewScheduler(baseBackoff, maxBackoff time.Duration, maxRetries int) *Scheduler {
	h := &jobHeap{}
	heap.Init(h)
	return &Scheduler{
		jobs:        h,
		newJob:      make(chan struct{}, 1),
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
		maxRetries:  maxRetries,
	}
}

func (s *Scheduler) MaxRetries() int {
	return s.maxRetries
}

func (s *Scheduler) Schedule(job Job) {
	s.mu.Lock()
	heap.Push(s.jobs, job)
	s.mu.Unlock()

	select {
	case s.newJob <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Start(ctx context.Context, wg *sync.WaitGroup, ready chan<- Job) {
	defer wg.Done()

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		s.mu.Lock()
		if s.jobs.Len() == 0 {
			s.mu.Unlock()
			select {
			case <-s.newJob:
				continue
			case <-ctx.Done():
				return
			}
		}

		nextJob := (*s.jobs)[0]
		now := time.Now()

		if now.After(nextJob.NextAttemptAt) || now.Equal(nextJob.NextAttemptAt) {
			job := heap.Pop(s.jobs).(Job)
			s.mu.Unlock()

			select {
			case ready <- job:
			default:
				// buffer is full
			}
			continue
		}

		delay := nextJob.NextAttemptAt.Sub(now)
		s.mu.Unlock()

		timer.Reset(delay)

		select {
		case <-timer.C:
		case <-s.newJob:
			if !timer.Stop() {
				<-timer.C
			}
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

// CalculateDelay returns the delay for the next attempt using exponential backoff with jitter.
func CalculateDelay(attempt int, base, max time.Duration) time.Duration {
	if attempt <= 0 {
		return base
	}

	delay := float64(base) * float64(uint(1)<<(attempt-1))
	if delay > float64(max) {
		delay = float64(max)
	}

	jitter := (rand.Float64() * 0.2) - 0.1 // +- 10%
	delay = delay * (1 + jitter)

	return time.Duration(delay)
}
