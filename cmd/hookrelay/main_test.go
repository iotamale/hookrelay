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
)

func TestSubscribeEndpoint_Validation(t *testing.T) {
	b := broker.NewBroker(10, 2)
	router := newRouter(b)

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
	b := broker.NewBroker(10, 2)
	router := newRouter(b)

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

	// Mock downstream webhook receiver
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read webhook body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		expectedSig := broker.SignPayload(body, secret)
		if gotSig := r.Header.Get("X-Hook-Signature-256"); gotSig != expectedSig {
			t.Errorf("invalid HMAC signature: got %s, want %s", gotSig, expectedSig)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		webhookReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	b := broker.NewBroker(16, 2)
	b.Start()
	router := newRouter(b)

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
