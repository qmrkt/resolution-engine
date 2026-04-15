package executors

import "time"

// API fetch defaults.
const (
	DefaultAPIFetchTimeout = 30 * time.Second
	DefaultDialTimeout     = 10 * time.Second
	MaxAPIResponseBytes    = 10 << 20 // 10 MB
	MaxRedirects           = 10
)

// LLM judge defaults.
const (
	DefaultLLMTimeout          = 60 * time.Second
	DefaultAnthropicMaxTokens  = 4096
	DefaultCompletionMaxTokens = 1024
	DefaultWebSearchMaxUses    = 5
)

// Human judge defaults.
const (
	DefaultHumanJudgeHTTPTimeout  = 15 * time.Second
	DefaultHumanJudgePollInterval = 5 * time.Second
	DefaultHumanJudgeTimeout      = 172800 // seconds
)

// Market evidence defaults.
const (
	DefaultMarketEvidenceHTTPTimeout = 15 * time.Second
)
