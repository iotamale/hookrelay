package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthMiddleware(t *testing.T) {
	dummy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name           string
		apiKey         string
		authHeader     string
		expectedStatus int
	}{
		{
			name:           "auth disabled when api key is empty",
			apiKey:         "",
			authHeader:     "",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "valid bearer token",
			apiKey:         "secret-key-123",
			authHeader:     "Bearer secret-key-123",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "wrong token",
			apiKey:         "secret-key-123",
			authHeader:     "Bearer wrong-key",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "missing authorization header",
			apiKey:         "secret-key-123",
			authHeader:     "",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "wrong scheme",
			apiKey:         "secret-key-123",
			authHeader:     "Basic secret-key-123",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "empty bearer value",
			apiKey:         "secret-key-123",
			authHeader:     "Bearer ",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "bearer prefix without space",
			apiKey:         "secret-key-123",
			authHeader:     "Bearersecret-key-123",
			expectedStatus: http.StatusUnauthorized,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := authMiddleware(tc.apiKey, dummy)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d", tc.expectedStatus, rec.Code)
			}
		})
	}
}

func TestAuthMiddleware_UnauthorizedResponse(t *testing.T) {
	dummy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := authMiddleware("secret", dummy)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}

	expectedBody := `{"error":"unauthorized"}`
	if got := rec.Body.String(); got != expectedBody {
		t.Errorf("expected body %q, got %q", expectedBody, got)
	}
}