package main

import (
	"bytes"
	"encoding/json"
	"hookrelay/internal/broker"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(q, w int) broker.Config {
	return broker.Config{
		QueueSize:                q,
		WorkerCount:              w,
		MaxRetries:               3,
		BaseBackoff:              1 * time.Second,
		MaxBackoff:               5 * time.Second,
		BreakerGCInterval:        1 * time.Hour,
		BreakerGCTTL:             24 * time.Hour,
		FailuresThreshold:        5,
		OpenStateTimeoutDuration: 1 * time.Minute,
	}
}

func TestSubscribeEndpoint_Validation(t *testing.T) {
	b := broker.NewBroker(testConfig(10, 2))
	router := newRouter(b, "")

	tests := []struct {
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "valid subscription request",
			body:           `{"topic":"orders","target_url":"http://localhost:9000/hook","secret":"secret"}`,
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "missing topic field",
			body:           `{"target_url":"http://localhost:9000/hook"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "missing target_url field",
			body:           `{"topic":"orders"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "malformed JSON payload",
			body:           `{"topic":"orders", "target_url":`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "empty request body",
			body:           ``,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/subscribe", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPublishEndpoint_Validation(t *testing.T) {
	b := broker.NewBroker(testConfig(10, 2))
	router := newRouter(b, "")

	tests := []struct {
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "valid publish request",
			body:           `{"topic":"orders","payload":{"id":123}}`,
			expectedStatus: http.StatusAccepted,
		},
		{
			name:           "missing topic field",
			body:           `{"payload":{"id":123}}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "malformed JSON payload",
			body:           `{invalid-json}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/publish", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestSubscribeAndPublish(t *testing.T) {
	const secret = "secret123"
	var webhookReceived atomic.Bool

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read webhook body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		expectedSig := broker.SignPayload(body, secret)
		if gotSig := r.Header.Get("X-Hook-Signature-256"); gotSig != expectedSig {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		webhookReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	b := broker.NewBroker(testConfig(16, 2))
	b.Start()
	router := newRouter(b, "")

	// Register subscriber via HTTP API
	subPayload, _ := json.Marshal(map[string]string{
		"topic":      "deployments",
		"target_url": receiver.URL,
		"secret":     secret,
	})
	subReq := httptest.NewRequest(http.MethodPost, "/v1/subscribe", bytes.NewReader(subPayload))
	subRec := httptest.NewRecorder()
	router.ServeHTTP(subRec, subReq)

	if subRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on subscribe, got %d", subRec.Code)
	}

	// Publish event via HTTP API
	pubPayload := `{"topic":"deployments","payload":{"version":"v1.0.0"}}`
	pubReq := httptest.NewRequest(http.MethodPost, "/v1/publish", bytes.NewBufferString(pubPayload))
	pubRec := httptest.NewRecorder()
	router.ServeHTTP(pubRec, pubReq)

	if pubRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on publish, got %d", pubRec.Code)
	}

	var pubResp struct {
		EventID          string `json:"event_id"`
		QueuedDeliveries int    `json:"queued_deliveries"`
	}
	if err := json.NewDecoder(pubRec.Body).Decode(&pubResp); err != nil {
		t.Fatalf("failed to decode publish response: %v", err)
	}
	if pubResp.QueuedDeliveries != 1 {
		t.Fatalf("expected 1 queued delivery, got %d", pubResp.QueuedDeliveries)
	}

	// Stop broker to flush all jobs
	b.Stop()

	if !webhookReceived.Load() {
		t.Fatal("expected downstream receiver to receive signed webhook, but it did not")
	}
}

func TestHealthReadyStatsEndpoints(t *testing.T) {
	b := broker.NewBroker(testConfig(10, 2))
	router := newRouter(b, "")

	// Test health
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Test ready before start
	req = httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	// Test ready after start
	b.Start()
	req = httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Test stats
	req = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	b.Stop()
}

func TestUnsubscribeEndpoint(t *testing.T) {
	b := broker.NewBroker(testConfig(10, 2))
	router := newRouter(b, "")

	b.Subscribe("topic1", broker.Subscriber{ID: "sub-1", TargetURL: "http://test.com"})

	tests := []struct {
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "valid unsubscribe request",
			body:           `{"topic":"topic1","target_url":"http://test.com"}`,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "unsubscribe non-existent",
			body:           `{"topic":"topic1","target_url":"http://test2.com"}`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "invalid body",
			body:           `{"topic":""}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodDelete, "/v1/subscribe", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestGetSubscribersEndpoint(t *testing.T) {
	b := broker.NewBroker(testConfig(10, 2))
	router := newRouter(b, "")

	b.Subscribe("topic1", broker.Subscriber{ID: "sub-1", TargetURL: "http://test.com"})

	// Test valid with query param
	req := httptest.NewRequest(http.MethodGet, "/v1/subscribers?topic=topic1", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Test missing topic
	req = httptest.NewRequest(http.MethodGet, "/v1/subscribers", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}
