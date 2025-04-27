package llm

import (
	"context"
	"databot-ai/internal/constants"
	"databot-ai/internal/models"
	"encoding/json"
	"fmt"
	"log"

	"github.com/sashabaranov/go-openai"
)

type OpenAIClient struct {
	client              *openai.Client
	model               string
	maxCompletionTokens int
	temperature         float64
	DBConfigs           []LLMDBConfig
}

func NewOpenAIClient(config Config) (*OpenAIClient, error) {
	if config.APIKey == "" {
		return nil, fmt.Errorf("OpenAI API key is required")
	}

	client := openai.NewClient(config.APIKey)
	model := config.Model
	if model == "" {
		model = openai.GPT4o
	}

	return &OpenAIClient{
		client:              client,
		model:               model,
		maxCompletionTokens: config.MaxCompletionTokens,
		temperature:         config.Temperature,
		DBConfigs:           config.DBConfigs,
	}, nil
}

func (c *OpenAIClient) GenerateResponse(ctx context.Context, messages []*models.LLMMessage, dbType string) (string, error) {
	// Check if the context is cancelled
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	// Convert messages to OpenAI format
	openAIMessages := make([]openai.ChatCompletionMessage, 0, len(messages))

	systemPrompt := ""
	responseSchema := ""

	for _, dbConfig := range c.DBConfigs {
		if dbConfig.DBType == dbType {
			systemPrompt = dbConfig.SystemPrompt
			responseSchema = dbConfig.Schema.(string)
			break
		}
	}

	// Add system message with database-specific prompt only
	openAIMessages = append(openAIMessages, openai.ChatCompletionMessage{
		Role:    "system",
		Content: systemPrompt,
	})

	// log.Printf("OPENAI -> GenerateResponse -> messages: %v", messages)

	for _, msg := range messages {
		content := ""

		// Handle different message types
		switch msg.Role {
		case "user":
			if userMsg, ok := msg.Content["user_message"].(string); ok {
				content = userMsg
			}
		case "assistant":
			content = formatAssistantResponse(msg.Content["assistant_response"].(map[string]interface{}))
		case "system":
			if schemaUpdate, ok := msg.Content["schema_update"].(string); ok {
				content = fmt.Sprintf("Database schema update:\n%s", schemaUpdate)
			}
		}

		if content != "" {
			openAIMessages = append(openAIMessages, openai.ChatCompletionMessage{
				Role:    mapRole(msg.Role),
				Content: content,
			})
		}
	}

	// Create completion request with JSON schema
	req := openai.ChatCompletionRequest{
		Model:               c.model,
		Messages:            openAIMessages,
		MaxCompletionTokens: c.maxCompletionTokens,
		Temperature:         float32(c.temperature),
		ResponseFormat: &openai.ChatCompletionResponseFormat{
			Type: openai.ChatCompletionResponseFormatTypeJSONSchema,
			JSONSchema: &openai.ChatCompletionResponseFormatJSONSchema{
				Name:        "databot-response",
				Description: "A friendly AI Response/Explanation or clarification question (Must Send this)",
				Schema:      json.RawMessage(responseSchema),
				Strict:      false,
			},
		},
	}

	// Check if the context is cancelled
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	// Call OpenAI API
	resp, err := c.client.CreateChatCompletion(ctx, req)
	if err != nil {
		log.Printf("GenerateResponse -> err: %v", err)
		return "", fmt.Errorf("OpenAI API error: %v", err)
	}

	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("no response from OpenAI")
	}

	log.Printf("OPENAI -> GenerateResponse -> resp: %v", resp)
	// Validate response against schema
	var llmResponse constants.LLMResponse
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &llmResponse); err != nil {
		return "", fmt.Errorf("invalid response format: %v", err)
	}

	return resp.Choices[0].Message.Content, nil
}

func (c *OpenAIClient) GetModelInfo() ModelInfo {
	return ModelInfo{
		Name:                c.model,
		Provider:            "openai",
		MaxCompletionTokens: c.maxCompletionTokens,
	}
}
