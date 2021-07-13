package stream

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

const region = "eu-west-1"

var testClient *dynamodb.DynamoDB

func init() {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials("fake", "accessKeyId", "secretKeyId"),
	})
	if err != nil {
		panic("can't create db_test session: " + err.Error())
	}
	testClient = dynamodb.New(sess)
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8000"
	}
	testClient.Endpoint = endpoint
}

func createLocalTable(t *testing.T) (name string) {
	name = uuid.New().String()
	_, err := testClient.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("_pk"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("_sk"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("_pk"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("_sk"),
				KeyType:       aws.String("RANGE"),
			},
		},
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
		TableName:   aws.String(name),
	})
	if err != nil {
		t.Fatalf("failed to create local table: %v", err)
	}
	return
}

func deleteLocalTable(t *testing.T, name string) {
	_, err := testClient.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("failed to delete table: %v", err)
	}
}
