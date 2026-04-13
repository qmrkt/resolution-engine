package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/question-market/resolution-engine/executors"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	// Configure slog: JSON when LOG_FORMAT=json, text otherwise.
	var handler slog.Handler
	if os.Getenv("LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	indexerURL := envOrDefault("INDEXER_URL", "http://localhost:3001")
	algodServer := envOrDefault("ALGOD_SERVER", "https://testnet-api.algonode.cloud")
	algodPort := envOrDefault("ALGOD_PORT", "443")
	algodToken := envOrDefault("ALGOD_TOKEN", "")
	anthropicKey := envOrDefault("ANTHROPIC_API_KEY", "")
	openAIKey := envOrDefault("OPENAI_API_KEY", "")
	googleKey := envOrDefault("GOOGLE_API_KEY", envOrDefault("GEMINI_API_KEY", ""))
	anthropicBaseURL := envOrDefault("ANTHROPIC_API_BASE_URL", "")
	openAIBaseURL := envOrDefault("OPENAI_API_BASE_URL", "")
	googleBaseURL := envOrDefault("GOOGLE_API_BASE_URL", envOrDefault("GEMINI_API_BASE_URL", ""))
	dataDir := envOrDefault("RESOLUTION_DATA_DIR", "data")
	pollIntervalStr := envOrDefault("POLL_INTERVAL", "30000")
	healthPort := envOrDefault("HEALTH_PORT", "3002")
	traceIngestToken := envOrDefault("INDEXER_WRITE_TOKEN", envOrDefault("TRACE_INGEST_TOKEN", ""))
	authorityPrivateKey := envOrDefault(
		"RESOLUTION_AUTHORITY_PRIVATE_KEY",
		envOrDefault("RESOLUTION_AUTHORITY_PRIVATE_KEY_B64", envOrDefault("AVM_PRIVATE_KEY", "")),
	)
	authorityMnemonic := envOrDefault("RESOLUTION_AUTHORITY_MNEMONIC", "")
	allowLocalAPIFetch := envOrDefault("RESOLUTION_ALLOW_LOCAL_API_FETCH", "") == "1"

	pollIntervalMs, _ := strconv.Atoi(pollIntervalStr)

	slog.Info("starting resolution engine", "component", "main")
	slog.Info("configuration",
		"component", "main",
		"indexer_url", indexerURL,
		"algod_server", algodServer,
		"algod_port", algodPort,
		"poll_interval_ms", pollIntervalMs,
	)
	if anthropicKey != "" {
		slog.Info("Anthropic API key configured", "component", "main")
	}
	if openAIKey != "" {
		slog.Info("OpenAI API key configured", "component", "main")
	}
	if googleKey != "" {
		slog.Info("Google API key configured", "component", "main")
	}
	if anthropicKey == "" && openAIKey == "" && googleKey == "" {
		slog.Warn("no llm provider API key configured, llm_judge will return inconclusive", "component", "main")
	}

	chainClient, err := NewAlgodMarketClient(indexerURL, algodServer, algodPort, algodToken)
	if err != nil {
		slog.Error("failed to create algod client", "component", "main", "error", err)
		os.Exit(1)
	}
	submitter, err := NewAuthoritySubmitter(chainClient, authorityPrivateKey, authorityMnemonic)
	if err != nil {
		slog.Error("failed to configure authority submitter", "component", "main", "error", err)
		os.Exit(1)
	}

	runner := NewRunner(executors.LLMJudgeExecutorConfig{
		AnthropicAPIKey:  anthropicKey,
		AnthropicBaseURL: anthropicBaseURL,
		OpenAIAPIKey:     openAIKey,
		OpenAIBaseURL:    openAIBaseURL,
		GoogleAPIKey:     googleKey,
		GoogleBaseURL:    googleBaseURL,
	}, indexerURL, dataDir, traceIngestToken)
	if allowLocalAPIFetch {
		apiFetch := executors.NewAPIFetchExecutor()
		apiFetch.AllowLocal = true
		runner.engine.RegisterExecutor("api_fetch", apiFetch)
		slog.Warn("local api_fetch is enabled; use only for trusted local smoke tests", "component", "main")
	}
	automation := NewMarketAutomationService(runner, chainClient, submitter)
	watcher := NewWatcher(indexerURL, time.Duration(pollIntervalMs)*time.Millisecond)

	// Start health HTTP server.
	startedAt := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		stats := watcher.Stats()
		status := "ok"
		if stats.LastPollAt.IsZero() {
			status = "degraded"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          status,
			"uptime_seconds":  int(time.Since(startedAt).Seconds()),
			"last_poll_at":    formatTimePtr(stats.LastPollAt),
			"markets_watched": stats.MarketsWatched,
			"indexer_url":     indexerURL,
		})
	})

	healthServer := &http.Server{
		Addr:    ":" + healthPort,
		Handler: mux,
	}
	go func() {
		slog.Info("health server listening", "component", "main", "port", healthPort)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server error", "component", "main", "error", err)
		}
	}()

	done := make(chan struct{})

	go watcher.Run(done,
		func(m MarketInfo) {
			slog.Debug("market active",
				"component", "main",
				slog.Int("app_id", m.AppID),
				slog.Int("deadline", m.Deadline),
			)
			automation.HandleActive(m)
		},
		func(m MarketInfo) {
			slog.Info("market pending resolution", "component", "main", slog.Int("app_id", m.AppID))
			automation.HandlePending(m)
		},
		func(m MarketInfo) {
			slog.Info("market ready to finalize", "component", "main", slog.Int("app_id", m.AppID))
			automation.HandleFinalizable(m)
		},
		func(m MarketInfo) {
			slog.Info("market disputed",
				"component", "main",
				slog.Int("app_id", m.AppID),
				slog.String("challenger", m.Challenger),
				slog.Int("challenge_reason_code", m.ChallengeReasonCode),
			)
			automation.HandleDisputed(m)
		},
	)

	// Graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("received signal, shutting down", "component", "main", "signal", sig.String())
	close(done)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("health server shutdown error", "component", "main", "error", err)
	}
}

func formatTimePtr(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
