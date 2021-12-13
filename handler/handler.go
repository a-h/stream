package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"go.uber.org/zap"
)

type eventBridgeAPI interface {
	PutEvents(context.Context, *eventbridge.PutEventsInput, ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

var log *zap.Logger
var eventBridge eventBridgeAPI
var eventBusName = os.Getenv("EVENT_BUS_NAME")
var eventSourceName = os.Getenv("EVENT_SOURCE_NAME")

func Start() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic("unable to load aws config: " + err.Error())
	}
	eventBridge = eventbridge.NewFromConfig(cfg)
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
	var outboundEvents []types.PutEventsRequestEntry
	for i := 0; i < len(event.Records); i++ {
		id, eventType, outboundEvent, err := createOutboundEvent(event.Records[i].Change.NewImage)
		if err != nil {
			return err
		}
		if outboundEvent == nil {
			continue
		}
		outboundEvents = append(outboundEvents, *outboundEvent)
		log.Info("found outbound event", zap.String("id", id), zap.String("type", eventType))
	}
	batches := batch(outboundEvents, 10)
	for i := 0; i < len(batches); i++ {
		log.Info("sending batch", zap.Int("batch", i+1), zap.Int("n", len(batches)))
		peo, err := eventBridge.PutEvents(context.Background(), &eventbridge.PutEventsInput{
			Entries: batches[i],
		})
		if err != nil {
			return fmt.Errorf("failed to send events: %v", err)
		}
		if peo.FailedEntryCount > 0 {
			return fmt.Errorf("failed to send %d events", peo.FailedEntryCount)
		}
	}
	log.Info("complete", zap.Int("sent", len(outboundEvents)))
	return nil
}

func createOutboundEvent(r map[string]events.DynamoDBAttributeValue) (id, eventType string, e *types.PutEventsRequestEntry, err error) {
	pkField, ok := r["_pk"]
	if !ok {
		return
	}
	id = pkField.String()
	skField, ok := r["_sk"]
	if !ok {
		return
	}
	sk := skField.String()
	if !strings.HasPrefix(sk, "OUTBOUND/") {
		return
	}
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
	// Strip type data.
	m, err := stripDynamoDBTypesFromMap(r)
	if err != nil {
		err = fmt.Errorf("could not strip dynamodb type information from record: %v", err)
		return
	}
	// Get JSON.
	detailJSON, err := json.Marshal(m)
	if err != nil {
		return
	}
	detail := string(detailJSON)

	e = &types.PutEventsRequestEntry{
		DetailType:   &eventType,
		EventBusName: &eventBusName,
		Source:       &eventSourceName,
		Detail:       &detail,
	}
	return
}

func stripDynamoDBTypesFromMap(m map[string]events.DynamoDBAttributeValue) (op map[string]interface{}, err error) {
	op = make(map[string]interface{})
	for k := range m {
		k := k
		op[k], err = getAttributeValue(m[k])
		if err != nil {
			return
		}
	}
	return
}

func stripDynamoDBTypesFromList(list []events.DynamoDBAttributeValue) (op []interface{}, err error) {
	op = make([]interface{}, len(list))
	for i := 0; i < len(list); i++ {
		op[i], err = getAttributeValue(list[i])
		if err != nil {
			return
		}
	}
	return
}

func getAttributeValue(av events.DynamoDBAttributeValue) (interface{}, error) {
	switch av.DataType() {
	case events.DataTypeBinary:
		return av.Binary(), nil
	case events.DataTypeBoolean:
		return av.Boolean(), nil
	case events.DataTypeBinarySet:
		return av.BinarySet(), nil
	case events.DataTypeList:
		return stripDynamoDBTypesFromList(av.List())
	case events.DataTypeMap:
		return stripDynamoDBTypesFromMap(av.Map())
	case events.DataTypeNumber:
		return getNumber(av.Number())
	case events.DataTypeNumberSet:
		return av.NumberSet(), nil
	case events.DataTypeNull:
		return nil, nil
	case events.DataTypeString:
		return av.String(), nil
	case events.DataTypeStringSet:
		return av.StringSet(), nil
	}
	return nil, fmt.Errorf("unknown DynamoDBAttributeValue type: %v", reflect.TypeOf(av.DataType()))
}

func getNumber(s string) (interface{}, error) {
	// First try integer.
	i, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return i, err
	}
	// Then float.
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return f, err
	}
	return nil, fmt.Errorf("cannot parse %q as int64 or float64", s)
}

func batch(values []types.PutEventsRequestEntry, n int) (pages [][]types.PutEventsRequestEntry) {
	for i := 0; i < len(values); i += n {
		limit := i + n
		if limit > len(values) {
			limit = len(values)
		}
		pages = append(pages, values[i:limit])
	}
	return
}
