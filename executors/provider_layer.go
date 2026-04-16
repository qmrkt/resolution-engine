package executors

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// providerLayerConfig centralizes provider credentials, base URLs, and shared
// endpoint resolution used by both llm_call and agent_loop executors.
type providerLayerConfig struct {
	AnthropicAPIKey  string
	AnthropicBaseURL string
	OpenAIAPIKey     string
	OpenAIBaseURL    string
	GoogleAPIKey     string
	GoogleBaseURL    string
	HTTPClient       *http.Client
}

func newProviderLayerConfig(cfg LLMCallExecutorConfig) providerLayerConfig {
	return providerLayerConfig{
		AnthropicAPIKey:  cfg.AnthropicAPIKey,
		AnthropicBaseURL: defaultString(cfg.AnthropicBaseURL, defaultAnthropicBaseURL),
		OpenAIAPIKey:     cfg.OpenAIAPIKey,
		OpenAIBaseURL:    defaultString(cfg.OpenAIBaseURL, defaultOpenAIBaseURL),
		GoogleAPIKey:     cfg.GoogleAPIKey,
		GoogleBaseURL:    defaultString(cfg.GoogleBaseURL, defaultGoogleBaseURL),
		HTTPClient:       cfg.HTTPClient,
	}
}

func (c providerLayerConfig) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c providerLayerConfig) apiKey(provider string) (string, error) {
	switch provider {
	case LLMProviderAnthropic:
		return c.AnthropicAPIKey, nil
	case LLMProviderOpenAI:
		return c.OpenAIAPIKey, nil
	case LLMProviderGoogle:
		return c.GoogleAPIKey, nil
	default:
		return "", fmt.Errorf("unsupported llm provider %q", provider)
	}
}

func (c providerLayerConfig) endpoint(provider string, model string) (string, error) {
	switch provider {
	case LLMProviderAnthropic:
		return c.AnthropicBaseURL, nil
	case LLMProviderOpenAI:
		return c.OpenAIBaseURL, nil
	case LLMProviderGoogle:
		return googleGenerateContentEndpoint(c.GoogleBaseURL, c.GoogleAPIKey, model), nil
	default:
		return "", fmt.Errorf("unsupported llm provider %q", provider)
	}
}

func googleGenerateContentEndpoint(baseURL string, apiKey string, model string) string {
	base := strings.TrimRight(defaultString(baseURL, defaultGoogleBaseURL), "/")
	return fmt.Sprintf("%s/%s:generateContent?key=%s", base, url.PathEscape(model), url.QueryEscape(apiKey))
}
