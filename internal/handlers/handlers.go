package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	"cm_water_openai/internal/config"
	"cm_water_openai/internal/openai"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Message struct {
	ID         string `json:"id"`
	Service    string `json:"service"`
	CreatedAt  string `json:"created_at"`
	RawMessage string `json:"raw_message"`
	Source     Source `json:"source"`
}

type Source struct {
	Channel    string `json:"channel"`
	SourceURI  string `json:"source_uri"`
	SenderName string `json:"sender_name"`
	SenderURI  string `json:"sender_uri"`
}

type OpenAIResponse struct {
	Organization     string    `json:"organization"`
	ShortDescription string    `json:"short_description"`
	Event            string    `json:"event"`
	EventStart       string    `json:"event_start"`
	EventStop        *string   `json:"event_stop"`
	Addresses        []Address `json:"addresses"`
}

type Address struct {
	City       string `json:"city"`
	StreetType string `json:"street_type"`
	Street     string `json:"street"`
	House      House  `json:"house"`
}

type House struct {
	Numbers []string   `json:"numbers"`
	Ranges  [][]string `json:"ranges"`
}

func Start(cfg *config.Config) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(cfg.AWSRegion),
	}))

	sqsSvc := sqs.New(sess)
	dynamoSvc := dynamodb.New(sess)

	for {
		receiveMessage(sqsSvc, dynamoSvc, cfg)
		time.Sleep(10 * time.Second)
	}
}

func receiveMessage(sqsSvc *sqs.SQS, dynamoSvc *dynamodb.DynamoDB, cfg *config.Config) {

	result, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(cfg.SQSQueueURL),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(10),
	})
	if err != nil {
		log.Printf("Error receiving message: %v", err)
		return
	}

	if len(result.Messages) == 0 {
		return
	}

	for _, message := range result.Messages {
		go handleMessage(dynamoSvc, sqsSvc, cfg, message)
	}
}

func handleMessage(dynamoSvc *dynamodb.DynamoDB, sqsSvc *sqs.SQS, cfg *config.Config, msg *sqs.Message) {
	log.Printf("Got message from queue")
	var m Message
	err := json.Unmarshal([]byte(*msg.Body), &m)
	if err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		return
	}

	response, err := openai.CallOpenAI(cfg.OpenAIAPIKey, m.RawMessage)
	if err != nil {
		log.Printf("Error calling OpenAI: %v", err)
		return
	}

	response = removeMarkdown(response)

	err = saveToDynamoDB(dynamoSvc, sqsSvc, cfg.SQSEnrichQueueURL, cfg.DynamoDBTableName, m.ID, response)
	if err != nil {
		log.Printf("Error saving to DynamoDB: %v", err)
		return
	}

	// Delete the message from SQS queue after successful processing
	_, err = sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(cfg.SQSQueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		log.Printf("Error deleting message from SQS: %v", err)
	}
}

func removeMarkdown(input string) string {
	re := regexp.MustCompile("```(?:json)?")
	return re.ReplaceAllString(input, "")
}

func saveToDynamoDB(dynamoSvc *dynamodb.DynamoDB, sqsSvc *sqs.SQS, queueName string, tableName string, id string, response string) error {
	var openAIResp OpenAIResponse
	err := json.Unmarshal([]byte(response), &openAIResp)
	if err != nil {
		return err
	}

	for i, address := range openAIResp.Addresses {
		item := map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
			"mp": {
				S: aws.String(fmt.Sprintf("water_mp:%s:%d", id, i+1)),
			},
			"organization": {
				S: aws.String(openAIResp.Organization),
			},
			"short_description": {
				S: aws.String(openAIResp.ShortDescription),
			},
			"event": {
				S: aws.String(openAIResp.Event),
			},
			"event_start": {
				S: aws.String(openAIResp.EventStart),
			},
			"city": {
				S: aws.String(address.City),
			},
			"street_type": {
				S: aws.String(address.StreetType),
			},
			"street": {
				S: aws.String(address.Street),
			},
		}

		if openAIResp.EventStop != nil {
			item["event_stop"] = &dynamodb.AttributeValue{
				S: aws.String(*openAIResp.EventStop),
			}
		}

		// Handle house numbers using StringSet
		if len(address.House.Numbers) > 0 {
			item["house_numbers"] = &dynamodb.AttributeValue{
				SS: aws.StringSlice(address.House.Numbers),
			}
		}

		// Handle house ranges using StringSet
		var rangeStrings []string
		for _, r := range address.House.Ranges {
			rangeStrings = append(rangeStrings, fmt.Sprintf("%s-%s", r[0], r[1]))
		}
		if len(rangeStrings) > 0 {
			item["house_ranges"] = &dynamodb.AttributeValue{
				SS: aws.StringSlice(rangeStrings),
			}
		}

		_, err = dynamoSvc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			return err
		}

		enrichmentMsg := map[string]interface{}{
			"id":                id,
			"mp":                fmt.Sprintf("water_mp:%s:%d", id, i+1),
			"organization":      openAIResp.Organization,
			"short_description": openAIResp.ShortDescription,
			"event":             openAIResp.Event,
			"event_start":       openAIResp.EventStart,
			"event_stop":        openAIResp.EventStop,
			"city":              address.City,
			"street_type":       address.StreetType,
			"street":            address.Street,
			"service":           "WATER",
			"house_numbers":     address.House.Numbers,
			"house_ranges":      rangeStrings,
		}

		msgBody, err := json.Marshal(enrichmentMsg)
		if err != nil {
			log.Printf("Error marshaling enrichment message: %v", err)
			continue
		}

		_, err = sqsSvc.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(queueName),
			MessageBody: aws.String(string(msgBody)),
		})
		if err != nil {
			log.Printf("Error sending enrichment message to SQS: %v", err)
		}
	}

	return nil
}
