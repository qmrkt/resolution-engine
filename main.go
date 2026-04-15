package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/question-market/resolution-engine/executors"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	var handler slog.Handler
	if os.Getenv("LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	indexerURL := envOrDefault("INDEXER_URL", "http://localhost:3001")
	listenPort := envOrDefault("LISTEN_PORT", "3002")
	dataDir := envOrDefault("RESOLUTION_DATA_DIR", "data")
	traceIngestToken := envOrDefault("INDEXER_WRITE_TOKEN", envOrDefault("TRACE_INGEST_TOKEN", ""))
	engineToken := envOrDefault("ENGINE_CONTROL_TOKEN", "")
	callbackToken := envOrDefault("ENGINE_CALLBACK_TOKEN", "")
	anthropicKey := envOrDefault("ANTHROPIC_API_KEY", "")
	openAIKey := envOrDefault("OPENAI_API_KEY", "")
	googleKey := envOrDefault("GOOGLE_API_KEY", envOrDefault("GEMINI_API_KEY", ""))
	anthropicBaseURL := envOrDefault("ANTHROPIC_API_BASE_URL", "")
	openAIBaseURL := envOrDefault("OPENAI_API_BASE_URL", "")
	googleBaseURL := envOrDefault("GOOGLE_API_BASE_URL", envOrDefault("GEMINI_API_BASE_URL", ""))
	allowLocalAPIFetch := envOrDefault("RESOLUTION_ALLOW_LOCAL_API_FETCH", "") == "1"

	slog.Info("starting blueprint execution service",
		"component", "main",
		"listen_port", listenPort,
		"indexer_url", indexerURL,
	)

	runner := NewRunner(executors.LLMJudgeExecutorConfig{
		AnthropicAPIKey:  anthropicKey,
		AnthropicBaseURL: anthropicBaseURL,
		OpenAIAPIKey:     openAIKey,
		OpenAIBaseURL:    openAIBaseURL,
		GoogleAPIKey:     googleKey,
		GoogleBaseURL:    googleBaseURL,
	}, indexerURL, dataDir, traceIngestToken)
	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()
	runner.SetContext(serviceCtx)
	defer runner.Close()

	if allowLocalAPIFetch {
		apiFetch := executors.NewAPIFetchExecutor()
		apiFetch.AllowLocal = true
		runner.engine.RegisterExecutor("api_fetch", apiFetch)
		slog.Warn("local api_fetch is enabled; use only for trusted local smoke tests", "component", "main")
	}

	manager, err := NewDurableRunManager(runner, dataDir, nil, callbackToken, DurableRunManagerConfig{})
	if err != nil {
		slog.Error("create durable run manager", "component", "main", "error", err)
		os.Exit(1)
	}
	defer manager.Close()
	server := NewEngineServer(manager, engineToken)

	httpServer := &http.Server{
		Addr:              ":" + listenPort,
		Handler:           server.Handler(),
		ReadHeaderTimeout: DefaultServerReadHeaderTimeout,
		ReadTimeout:       DefaultServerReadTimeout,
		WriteTimeout:      DefaultServerWriteTimeout,
		IdleTimeout:       DefaultServerIdleTimeout,
	}
	go func() {
		slog.Info("engine server listening", "component", "main", "port", listenPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("engine server error", "component", "main", "error", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("received signal, shutting down", "component", "main", "signal", sig.String())
	serviceCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("http server shutdown error", "component", "main", "error", err)
	}
}
