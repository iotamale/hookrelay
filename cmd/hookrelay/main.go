package main

import (
	"context"
	"encoding/json"
	"errors"
	"hookrelay/internal/broker"
	"hookrelay/internal/config"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"uuid"

	"github.com/lmittmann/tint"
)

type subscribeRequest struct {
	Topic     string `json:"topic"`
	TargetURL string `json:"target_url"`
	Secret    string `json:"secret"`
}

type publishRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}



func newRouter(b *broker.Broker, apiKey string) http.Handler {
	// ------------------------ PUBLIC ENDPOINTS ------------------------
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"up"}`))
	})

	mux.HandleFunc("GET /v1/ready", func(w http.ResponseWriter, r *http.Request) {
		running := b.IsRunning()

		if running {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ready"}`))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"not_ready"}`))
		}
	})

	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := b.GetStats()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(stats)
	})

	// ------------------------ PROTECTED ENDPOINTS ------------------------
	protectedMux := http.NewServeMux()

	protectedMux.HandleFunc("POST /v1/subscribe", func(w http.ResponseWriter, r *http.Request) {
		var req subscribeRequest
		err := json.NewDecoder(r.Body).Decode(&req)

		if err != nil || req.TargetURL == "" || req.Topic == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"invalid request"}`))
			return
		}

		sub := broker.Subscriber{
			ID:        uuid.NewV7().String(),
			TargetURL: req.TargetURL,
			Secret:    req.Secret,
		}

		b.Subscribe(req.Topic, sub)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(sub)
	})

	protectedMux.HandleFunc("DELETE /v1/subscribe", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		targetURL := r.URL.Query().Get("target_url")

		if topic == "" || targetURL == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"missing topic or target_url query parameter"}`))
			return
		}

		removed := b.UnsubscribeByUrl(topic, targetURL)

		if removed {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"no subscriber meets specified params"}`))
		}
	})

	protectedMux.HandleFunc("GET /v1/subscribers", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")

		if topic == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"invalid request"}`))
			return
		}

		subs := b.GetSubscribers(topic)
		if subs == nil {
			subs = []broker.Subscriber{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"subscribers": subs,
			"topic":       topic,
		})
	})

	protectedMux.HandleFunc("POST /v1/publish", func(w http.ResponseWriter, r *http.Request) {
		var req publishRequest
		err := json.NewDecoder(r.Body).Decode(&req)

		if err != nil || req.Topic == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"invalid request"}`))
			return
		}

		evt := broker.Event{
			ID:        uuid.NewV7().String(),
			Topic:     req.Topic,
			Payload:   req.Payload,
			Timestamp: time.Now().UTC(),
		}

		queued := b.Publish(evt)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"event_id":          evt.ID,
			"queued_deliveries": queued,
		})
	})

	mux.Handle("/", authMiddleware(apiKey, protectedMux))

	return mux
}

func main() {
	appCfg := config.Load()
	var slogHandler slog.Handler

	switch appCfg.SlogOutputFormat {
	case "json":
		slogHandler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	case "text":
		slogHandler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	default: // "tint"
		slogHandler = tint.NewTextHandler(os.Stdout, &tint.Options{
			Level:      slog.LevelInfo,
			TimeFormat: time.TimeOnly,
		})
	}

	logger := slog.New(slogHandler)
	slog.SetDefault(logger)

	brokerCfg := broker.Config{
		QueueSize:                appCfg.QueueSize,
		WorkerCount:              appCfg.WorkerCount,
		MaxRetries:               appCfg.MaxRetries,
		BaseBackoff:              appCfg.BaseBackoff,
		MaxBackoff:               appCfg.MaxBackoff,
		BreakerGCInterval:        appCfg.BreakerGCInterval,
		BreakerGCTTL:             appCfg.BreakerGCTTL,
		FailuresThreshold:        appCfg.FailuresThreshold,
		OpenStateTimeoutDuration: appCfg.OpenStateTimeoutDuration,
	}

	b := broker.NewBroker(brokerCfg)
	b.Start()

	addr := ":" + appCfg.Port
	if appCfg.Port[0] == ':' {
		addr = appCfg.Port
	}

	httpHandler := maxBodyMiddleware(1<<20, newRouter(b, appCfg.APIKey))
	server := &http.Server{
		Addr:    addr,
		Handler: httpHandler,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("HookRelay server listening", "addr", server.Addr, "workers", appCfg.WorkerCount, "queue_size", appCfg.QueueSize)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = server.Shutdown(shutdownCtx)
	b.Stop()
	slog.Info("server stopped")
}
