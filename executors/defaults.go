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
	DefaultMapBatchSize               = 1
	DefaultMapMaxConcurrency          = 1
	DefaultMapMaxItems                = 100
	DefaultMapMaxDepth                = 4
	DefaultMapBatchInputKey           = "batch"
	DefaultMapBatchIndexInputKey      = "batch_index"
	DefaultMapBatchStartIndexInputKey = "batch_start_index"
	DefaultMapBatchEndIndexInputKey   = "batch_end_index"
	DefaultMapBatchItemCountInputKey  = "batch_item_count"
	DefaultMapOnError                 = "fail"
)
