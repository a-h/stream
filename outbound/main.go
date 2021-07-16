package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"go.uber.org/zap"
)

var log *zap.Logger
var sess = session.Must(session.NewSession())
var eventBridge = eventbridge.New(sess)
var eventBusName = os.Getenv("EVENT_BUS_NAME")
var eventSourceName = os.Getenv("EVENT_SOURCE_NAME")

func main() {
	var err error
	log, err = zap.NewProduction()
	if err != nil {
		panic("failed to create logger: " + err.Error())
	}
	if eventBusName == "" {
		panic("missing EVENT_BUS_NAME environment variable")
	}
	if eventSourceName == "" {
		panic("missing EVENT_SOURCE_NAME environment variable")
	}
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context, event events.DynamoDBEvent) error {
	defer log.Sync()
	log.Info("processing records", zap.Int("count", len(event.Records)))
	var outboundEvents []*eventbridge.PutEventsRequestEntry
	for i := 0; i < len(event.Records); i++ {
		id, eventType, outboundEvent := createOutboundEvent(event.Records[i].Change.NewImage)
		if outboundEvent == nil {
			continue
		}
		outboundEvents = append(outboundEvents, outboundEvent)
		log.Info("found outbound event", zap.String("id", id), zap.String("type", eventType))
	}
	batches := batch(outboundEvents, 10)
	for i := 0; i < len(batches); i++ {
		log.Info("sending batch", zap.Int("batch", i+1), zap.Int("n", len(batches)))
		peo, err := eventBridge.PutEvents(&eventbridge.PutEventsInput{
			Entries: batches[i],
		})
		if err != nil {
			return fmt.Errorf("failed to send events: %v", err)
		}
		if *peo.FailedEntryCount > 0 {
			return fmt.Errorf("failed to send %d events", *peo.FailedEntryCount)
		}
	}
	log.Info("complete", zap.Int("sent", len(outboundEvents)))
	return nil
}

func createOutboundEvent(r map[string]events.DynamoDBAttributeValue) (id, eventType string, e *eventbridge.PutEventsRequestEntry) {
	pkField, ok := r["_pk"]
	if !ok {
		return
	}
	id = pkField.String()
	typ, ok := r["_typ"]
	if !ok {
		return
	}
	eventType = typ.String()

	// Remove _ fields from the event.
	var keysToDelete []string
	for k := range r {
		k := k
		if strings.HasPrefix(k, "_") {
			keysToDelete = append(keysToDelete, k)
		}
	}
	for i := 0; i < len(keysToDelete); i++ {
		delete(r, keysToDelete[i])
	}
	detailJSON, err := json.Marshal(r)
	if err != nil {
		return
	}
	detail := string(detailJSON)

	e = &eventbridge.PutEventsRequestEntry{
		DetailType:   &eventType,
		EventBusName: &eventBusName,
		Source:       &eventSourceName,
		Detail:       &detail,
	}
	return
}

func batch(values []*eventbridge.PutEventsRequestEntry, n int) (pages [][]*eventbridge.PutEventsRequestEntry) {
	for i := 0; i < len(values); i += n {
		limit := i + n
		if limit > len(values) {
			limit = len(values)
		}
		pages = append(pages, values[i:limit])
	}
	return
}
