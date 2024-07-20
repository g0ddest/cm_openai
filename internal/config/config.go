package config

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	DynamoDBTableName  string `envconfig:"DYNAMODB_TABLE_NAME"`
	SQSQueueURL        string `envconfig:"SQS_QUEUE_URL"`
	AWSRegion          string `envconfig:"AWS_REGION"`
	AWSAccessKeyID     string `envconfig:"AWS_ACCESS_KEY_ID"`
	AWSSecretAccessKey string `envconfig:"AWS_SECRET_ACCESS_KEY"`
	OpenAIAPIKey       string `envconfig:"OPENAI_API_KEY"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
