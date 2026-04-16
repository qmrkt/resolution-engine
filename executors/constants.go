package executors

const (
	LLMProviderAnthropic = "anthropic"
	LLMProviderOpenAI    = "openai"
	LLMProviderGoogle    = "google"

	defaultAnthropicModel = "claude-sonnet-4-6"
	defaultOpenAIModel    = "gpt-5.4"
	defaultGoogleModel    = "gemini-3.1-pro-preview"

	defaultAnthropicBaseURL       = "https://api.anthropic.com/v1/messages"
	defaultOpenAIBaseURL          = "https://api.openai.com/v1/chat/completions"
	defaultOpenAIResponsesBaseURL = "https://api.openai.com/v1/responses"
	defaultGoogleBaseURL          = "https://generativelanguage.googleapis.com/v1beta/models"
)

const (
	contentPartTypeText       = "text"
	contentPartTypeToolUse    = "tool_use"
	contentPartTypeToolResult = "tool_result"
)

const (
	localhostHost       = "localhost"
	googleMetadataHost  = "metadata.google.internal"
	cloudMetadataIPAddr = "169.254.169.254"
)
