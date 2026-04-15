package main

import "time"

// Durable run manager defaults.
const (
	DefaultMaxWorkers   = 4
	DefaultMaxQueueSize = 1024
	DefaultPollInterval = time.Second
)

// Run manager defaults.
const (
	DefaultRunTTL             = 30 * time.Minute
	DefaultRunCleanupInterval = time.Minute
)

// Runner defaults.
const (
	DefaultRunTimeout = 7 * 24 * time.Hour
)

// HTTP client defaults.
const (
	DefaultHTTPClientTimeout  = 10 * time.Second
	DefaultShutdownTimeout    = 5 * time.Second
	DefaultTraceHTTPTimeout   = 5 * time.Second
	DefaultCallbackMaxBackoff = 60 * time.Second
)

// HTTP server defaults.
const (
	DefaultServerReadHeaderTimeout = 10 * time.Second
	DefaultServerReadTimeout       = 30 * time.Second
	DefaultServerWriteTimeout      = 30 * time.Second
	DefaultServerIdleTimeout       = 120 * time.Second
)

// Trace emitter defaults.
const (
	DefaultTraceQueueSize = 64
)

// Blueprint validation limits.
const (
	MaxBlueprintNodes = 16
	MaxBlueprintBytes = 8192
)

// Durable await-signal default timeout (seconds).
const DefaultDurableAwaitSignalTimeout = 172800
