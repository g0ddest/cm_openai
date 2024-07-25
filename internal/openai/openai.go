package openai

import (
	"context"
	"errors"
	"fmt"
	"github.com/sashabaranov/go-openai"
)

func CallOpenAI(apiKey, rawMessage string) (string, error) {
	prompt := "Given a text message describing a water supply event, generate a JSON object according to the following schema: {\"type\":\"object\",\"properties\":{\"organization\":{\"type\":\"string\"},\"short_description\":{\"type\":\"string\"},\"event\":{\"type\":\"string\",\"enum\":[\"shutdown\",\"resume\"]},\"event_start\":{\"type\":\"string\",\"format\":\"iso date-time without tz\"},\"event_stop\":{\"type\":\"string\",\"format\":\"iso date-time without tz\"},\"addresses\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\"},\"street_type\":{\"type\":\"string\",\"enum\":[\"ул.\",\"пер.\",\"пл.\"]},\"street\":{\"type\":\"string\"},\"house\":{\"type\":\"object\",\"properties\":{\"numbers\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"ranges\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}}}}}}} The JSON output must accurately reflect the details from the message. Response - ONLY JSON. Ignore named places, we need only addresses. Message starts with organization name usually. Be accurate with addresses."

	client := openai.NewClient(apiKey)
	resp, err := client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model: openai.GPT4oMini,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleUser,
					Content: prompt + "\n\n" + rawMessage,
				},
			},
		},
	)

	if err != nil {
		fmt.Printf("ChatCompletion error: %v\n", err)
		return "", errors.New("failed to get a successful response from OpenAI")
	}

	return resp.Choices[0].Message.Content, nil
}
