package broker

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"hookrelay/internal/retry"
)

func (b *Broker) worker() {
	defer b.wg.Done()

	for job := range b.jobs {
		b.deliver(job)
		b.processedCount.Add(1)
	}
}

func (b *Broker) Publish(evt Event) int {
	select {
	case <-b.ctx.Done():
		// Broker is being shutdown, prevent writing to closed b.jobs channel
		return 0
	default:
	}

	subs := b.GetSubscribers(evt.Topic)
	queued := 0

	for _, sub := range subs {
		deliveryJob := retry.Job{
			Payload: DeliveryJob{
				Subscriber: sub,
				Event:      evt,
			},
		}

		select {
		case b.jobs <- deliveryJob:
			queued++
		default:
			slog.Warn("buffer is full, job skipped", "event_id", evt.ID, "topic", evt.Topic, "target_url", deliveryJob.Payload.(DeliveryJob).Subscriber.TargetURL)
		}
	}

	return queued
}

func (b *Broker) deliver(job retry.Job) {
	payload := job.Payload.(DeliveryJob)
	sub := payload.Subscriber
	evt := payload.Event

	cb := b.breakerManager.GetBreaker(sub.TargetURL)

	if !cb.Allow() {
		b.handleRetry(job)
		return
	}

	body, err := json.Marshal(evt)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, sub.TargetURL, bytes.NewReader(body))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hook-Event-ID", evt.ID)
	req.Header.Set("X-Hook-Topic", evt.Topic)

	if sub.Secret != "" {
		hashString := SignPayload(body, sub.Secret)
		req.Header.Set("X-Hook-Signature-256", hashString)
	}

	resp, err := b.client.Do(req)

	isTransient := false

	if err != nil {
		slog.Warn("network error during delivery", "event_id", evt.ID, "url", sub.TargetURL, "error", err.Error())
		cb.RecordFailure()
		isTransient = true
		b.FailedAttempts.Add(1)
	} else {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			cb.RecordSuccess()
			slog.Info("webhook delivered successfully", "event_id", evt.ID, "target_url", sub.TargetURL, "status", resp.StatusCode, "attempt", job.Attempt)
			b.successCount.Add(1)
			return
		}

		if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500 {
			slog.Warn("upstream server error", "event_id", evt.ID, "url", sub.TargetURL, "status", resp.StatusCode)
			cb.RecordFailure()
			isTransient = true
			b.FailedAttempts.Add(1)
		} else if resp.StatusCode == http.StatusTooManyRequests {
			slog.Warn("rate limited by upstream", "event_id", evt.ID, "url", sub.TargetURL)
			isTransient = true
			b.FailedAttempts.Add(1)
		} else {
			slog.Warn("permanent client error, dropping webhook", "event_id", evt.ID, "target_url", sub.TargetURL, "status", resp.StatusCode)
			b.droppedCount.Add(1)
			return
		}
	}

	if isTransient {
		b.handleRetry(job)
	}
}

func (b *Broker) handleRetry(job retry.Job) {
	payload := job.Payload.(DeliveryJob)
	sub := payload.Subscriber
	evt := payload.Event

	if job.Attempt < b.cfg.MaxRetries {
		job.Attempt++
		delay := retry.CalculateDelay(job.Attempt, b.cfg.BaseBackoff, b.cfg.MaxBackoff)
		job.NextAttemptAt = time.Now().Add(delay)

		slog.Info("transient error, scheduling retry", "event_id", evt.ID, "target_url", sub.TargetURL, "attempt", job.Attempt, "delay", delay)
		b.retryScheduler.Schedule(job)
	} else {
		slog.Warn("exhausted retries, dropping webhook", "event_id", evt.ID, "target_url", sub.TargetURL)
		b.droppedCount.Add(1)
	}
}

func SignPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)

	hashString := hex.EncodeToString(mac.Sum(nil))
	return "sha256=" + hashString
}
