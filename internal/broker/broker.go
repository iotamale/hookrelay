package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hookrelay/internal/breaker"
	"hookrelay/internal/retry"
)

type Stats struct {
	SuccessfulDeliveries uint64 `json:"successful_deliveries"`
	FailedAttempts       uint64 `json:"failed_attempts"`
	DroppedDeliveries    uint64 `json:"dropped_deliveries"`
	JobQueueSize         int    `json:"job_queue_size"`
}

type Config struct {
	QueueSize                int
	WorkerCount              int
	MaxRetries               int
	BaseBackoff              time.Duration
	MaxBackoff               time.Duration
	BreakerGCInterval        time.Duration
	BreakerGCTTL             time.Duration
	FailuresThreshold        uint16
	OpenStateTimeoutDuration time.Duration
}

type Subscriber struct {
	ID        string `json:"id"`
	TargetURL string `json:"target_url"`
	Secret    string `json:"-"`
}

type Event struct {
	ID        string          `json:"id"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

type DeliveryJob struct {
	Subscriber Subscriber
	Event      Event
}

type Broker struct {
	cfg            Config
	mu             sync.RWMutex
	subscribers    map[string][]Subscriber // topic -> []Subscribers
	jobs           chan retry.Job
	wg             sync.WaitGroup // tracks active worker goroutines
	schedulerWg    sync.WaitGroup // tracks background goroutines
	workerCount    int
	processedCount atomic.Int64
	successCount   atomic.Uint64
	droppedCount   atomic.Uint64
	FailedAttempts atomic.Uint64
	client         *http.Client

	retryScheduler *retry.Scheduler
	breakerManager *breaker.Manager

	ctx       context.Context
	cancel    context.CancelFunc
	isRunning atomic.Bool
}

func NewBroker(cfg Config) *Broker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Broker{
		cfg:         cfg,
		subscribers: make(map[string][]Subscriber),
		jobs:        make(chan retry.Job, cfg.QueueSize),
		workerCount: cfg.WorkerCount,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		retryScheduler: retry.NewScheduler(cfg.BaseBackoff, cfg.MaxBackoff, cfg.MaxRetries),
		breakerManager: breaker.NewManager(cfg.FailuresThreshold, cfg.OpenStateTimeoutDuration, cfg.BreakerGCInterval, cfg.BreakerGCTTL),
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (b *Broker) Start() {
	slog.Info("starting broker")

	b.schedulerWg.Add(1)
	go b.retryScheduler.Start(b.ctx, &b.schedulerWg, b.jobs)

	b.schedulerWg.Add(1)
	go b.breakerManager.StartGC(b.ctx, &b.schedulerWg)

	slog.Info("initializing worker pool", "worker_count", b.workerCount)
	for i := 0; i < b.workerCount; i++ {
		b.wg.Add(1)
		go b.worker()
	}

	b.isRunning.Store(true)
}

func (b *Broker) Stop() {
	b.isRunning.Store(false)
	slog.Info("shutting down broker")

	// Cancel background routines (retry scheduler, breaker GC)
	b.cancel()
	b.schedulerWg.Wait()

	// Close the main job queue
	close(b.jobs)
	b.wg.Wait()

	slog.Info("broker shutdown complete", "processed_events", b.ProcessedCount())
}

func (b *Broker) ProcessedCount() int64 {
	return b.processedCount.Load()
}

func (b *Broker) GetStats() Stats {
	return Stats{
		SuccessfulDeliveries: b.successCount.Load(),
		FailedAttempts:       b.FailedAttempts.Load(),
		DroppedDeliveries:    b.droppedCount.Load(),
		JobQueueSize:         len(b.jobs),
	}
}

func (b *Broker) IsRunning() bool {
	return b.isRunning.Load()
}
