# HookRelay

![CI](https://github.com/iotamale/hookrelay/actions/workflows/ci.yml/badge.svg)

HookRelay is a lightweight, concurrent webhook delivery broker written in Go.

I built this project to get hands-on experience with concurrent programming and distributed system patterns. Sending a webhook is easy, but reliably delivering it when the downstream server is slow, rate-limiting you, or completely dead is an engineering challenge.

## Under the Hood

Instead of just building a standard CRUD app, I wanted to tackle some real-world reliability problems:

- **Circuit Breaker:** I implemented a custom circuit breaker with a background GC. It prevents the broker from hammering dead endpoints and transitions to a _Half-Open_ state to test recovery.
- **Smart Retries & Min-Heap:** Implemented a custom min-heap to efficiently schedule retries `O(log n)`. It uses exponential backoff with jitter and strictly honors the upstream `Retry-After` HTTP headers.
- **Concurrency:** The core runs on a non-blocking worker pool (Goroutines & Channels) with atomic counters, making it safe and fast.
- **Security:** Outbound payloads are signed using HMAC SHA-256 (`X-Hook-Signature-256`), and the control API is protected by a static Bearer token.

## How it works

Here is how a webhook flows through the app:

1. **Inbound:** When an event hits the API, it's shoved directly into a buffered `jobs` channel so the HTTP handler can return a `202 Accepted` immediately.
2. **Delivery (Worker Pool):** A number of dedicated goroutines constantly listen to `jobs` channel and attempt to send the actual HTTP requests.
3. **Circuit Breaker:** Before making the request, the worker checks with CB whether the URL is healthy. If the target server is known to be dead, we abort early.
4. **Retry Loop:** If the delivery fails, the job gets passed to the Retry Scheduler and it is placed in a min-heap. Once the exponential backoff timer expires, it is pushed back into the `jobs` channel for another attempt.

## Getting Started

You'll need Go 1.27.1+.

```bash
git clone https://github.com/yourusername/hookrelay.git
cd hookrelay
go run ./cmd/hookrelay
```

The server will start on port `8080`.

## Configuration

You can configure the broker via `.env` or environment variables:

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

Provide your API key via `Authorization: Bearer <API_KEY>`.

### Managing Subscriptions (Protected)

- `POST /v1/subscribe` - Register a new URL for a topic.
    ```json
    { "topic": "chat", "target_url": "https://api.example.com/webhook", "secret": "optional-hmac-secret" }
    ```
- `GET /v1/subscribers?topic=chat` - List active subscribers.
- `DELETE /v1/subscribe?topic=chat&target_url=https://api.example.com/webhook` - Remove a subscriber.

### Publishing Events (Protected)

- `POST /v1/publish` - Broadcast an event to all subscribers of a topic.
    ```json
    { "topic": "chat", "payload": { "username": "miguel1222", "message": "hello!" } }
    ```

### Telemetry (Public)

- `GET /v1/stats` - Internal metrics (successes, failures, dropped events, queue size).
- `GET /v1/health` - Basic liveness probe.
- `GET /v1/ready` - Readiness probe (returns 200 OK only when workers are running).

## Roadmap

- Replace in-memory maps with a durable datastore (SQLite or PostgreSQL) to survive restarts.
- Introduce a dead letter queue to log permanantly failed webhooks.
