package executors

import "time"

// API fetch defaults.
const (
	DefaultAPIFetchTimeout = 30 * time.Second
	DefaultDialTimeout     = 10 * time.Second
	MaxAPIResponseBytes    = 10 << 20 // 10 MB
	MaxRedirects           = 10
)

// LLM call defaults.
const (
	DefaultLLMTimeout          = 60 * time.Second
	DefaultAnthropicMaxTokens  = 4096
	DefaultCompletionMaxTokens = 1024
	DefaultWebSearchMaxUses    = 5
)

// Map executor defaults.
const (
	DefaultMapMaxConcurrency = 4
	DefaultMapMaxItems       = 100
	DefaultMapItemInputKey   = "item"
	DefaultMapIndexInputKey  = "item_index"
	DefaultMapMode           = "parallel"
	DefaultMapOnError        = "fail"
)
