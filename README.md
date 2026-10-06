# HookRelay

![CI](https://github.com/iotamale/hookrelay/actions/workflows/ci.yml/badge.svg)

**HookRelay** is a high-performance, concurrent webhook delivery broker written in Go.

It acts as an intermediary layer that dispatches events to multiple subscribers, handling transient network errors, rate limits, and slow downstream services without degrading the core application's performance.

## Key Features

- **Smart Retries:** Implements **exponential backoff with jitter** to safely retry failed deliveries. It natively parses and honors upstream `Retry-After` HTTP headers (supports both seconds and HTTP-date formats).
- **Resilient Delivery:** Failing endpoints trip the breaker, transitioning to a _Half-Open_ state for probing after a cooldown period. Includes a background Garbage Collector to clean up stale circuit breakers.
- **Efficient Job Scheduling:** Utilizes a custom **min-heap** data structure paired for O(log n) retry scheduling.
- **Highly Concurrent:** Built around a non-blocking Worker Pool architecture utilizing Go channels and atomic operations for safe state management.
- **Observability:** Structured JSON logging using the standard library `log/slog`.
- **Security First:**
    - API Key authentication via `Bearer` token middleware.
    - Payloads delivered to downstream services are signed using **HMAC SHA-256** (`X-Hook-Signature-256` header).
    - Request body size limiting.

## Getting Started

### Prerequisites

- Go 1.22 or higher

### Local Development

```bash
# Clone the repository
git clone https://github.com/yourusername/hookrelay.git
cd hookrelay

# Run the application
go run ./cmd/hookrelay
```

The server will start on port `8080` by default.

## Configuration

Configuration is managed via environment variables or a `.env` file.

| Variable             | Default   | Description                                         |
| -------------------- | --------- | --------------------------------------------------- |
| `PORT`               | `8080`    | HTTP server port                                    |
| `API_KEY`            | _(empty)_ | Secret key for Bearer auth (auth disabled if empty) |
| `WORKER_COUNT`       | `8`       | Number of concurrent delivery workers               |
| `QUEUE_SIZE`         | `1024`    | Size of the main delivery channel buffer            |
| `MAX_RETRIES`        | `13`      | Maximum number of retry attempts per webhook        |
| `BASE_BACKOFF`       | `1s`      | Initial delay for exponential backoff               |
| `MAX_BACKOFF`        | `1h`      | Maximum delay cap for exponential backoff           |
| `FAILURES_THRESHOLD` | `5`       | Failures needed to trip the Circuit Breaker         |

## API Reference

### Authentication

All business-logic endpoints are protected by a static API key. You must include the API key in the `Authorization` header:
`Authorization: Bearer <API_KEY>`

### Protected endpoints

#### `POST /v1/publish`

Publishes a new event to be delivered to all subscribers of a specific topic.

```json
{
	"topic": "orders.created",
	"payload": { "order_id": 9912, "status": "paid" }
}
```

#### `POST /v1/subscribe`

Registers a new subscriber for a topic.

```json
{
	"topic": "orders.created",
	"target_url": "https://api.example.com/webhook",
	"secret": "optional-hmac-secret-for-signing"
}
```

#### `DELETE /v1/subscribe?topic=...&target_url=...`

Removes a subscription.

#### `GET /v1/subscribers?topic=...`

Returns a list of active subscribers for a given topic.

### Public endpoints

- `GET /v1/stats` - Returns current internal broker metrics (successes, failures, dropped events, and current queue size).
- `GET /v1/health` - Returns 200 OK if the HTTP server is responsive.
- `GET /v1/ready` - Returns 200 OK only if the internal worker pool has successfully started and is ready to process jobs (returns 503 during graceful shutdown).

## Architecture Overview

1. **Ingress:** HTTP requests are routed via standard `net/http` mux.
2. **Broker Engine:** Events are placed into a buffered channel `b.jobs`.
3. **Workers:** A predefined number of Goroutines continuously pull from `b.jobs` and attempt HTTP POST deliveries.
4. **Resilience Layer:** If an upstream server timeouts, the `Circuit Breaker` records the failure. The event is pushed to the `retryHeap` and picked up by a dedicated scheduler for a future retry based on an exponential backoff algorithm.

## Roadmap

This project is continuously evolving. Planned improvements include:

- Moving away from in-memory maps to a durable datastore (e.g., PostgreSQL or Redis) to ensure zero data loss during server restarts.
- Dead Letter Queue: Storing permanently failed webhooks (max retries exceeded) for manual inspection and replay.
