package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"go.uber.org/multierr"
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
	var err error
	log, err = zap.NewProduction()
	if err != nil {
		panic("failed to create logger: " + err.Error())
	}
	if eventBusName == "" {
		log.Fatal("missing EVENT_BUS_NAME environment variable")
	}
	if eventSourceName == "" {
		log.Fatal("missing EVENT_SOURCE_NAME environment variable")
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal("unable to load aws config", zap.Error(err))
	}
	eventBridge = eventbridge.NewFromConfig(cfg)
	log.Info("starting handler")
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context, event events.DynamoDBEvent) error {
	defer log.Sync()
	//TODO: Remove.
	log.Info("processing records", zap.Int("count", len(event.Records)), zap.Any("event", event))
	var outboundEvents []types.PutEventsRequestEntry
	for i := 0; i < len(event.Records); i++ {
		id, eventType, outboundEvent, err := createOutboundEvent(event.Records[i].Change.NewImage)
		if err != nil {
			log.Error("failed to create outbound event", zap.Error(err))
			return err
		}
		if outboundEvent == nil {
			continue
		}
		outboundEvents = append(outboundEvents, *outboundEvent)
		log.Info("found outbound event", zap.String("id", id), zap.String("type", eventType))
	}
	batches, err := batch(outboundEvents)
	if err != nil {
		return fmt.Errorf("failed to create batches: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(len(batches))
	errors := make([]error, len(batches))
	for i := 0; i < len(batches); i++ {
		go func(i int) {
			defer wg.Done()
			log.Info("sending batch", zap.Int("batch", i+1), zap.Int("n", len(batches)))
			peo, err := eventBridge.PutEvents(context.Background(), &eventbridge.PutEventsInput{
				Entries: batches[i],
			})
			if err != nil {
				errors[i] = fmt.Errorf("batch %d: failed to send events: %v", i, err)
				return
			}
			if peo.FailedEntryCount > 0 {
				errors[i] = fmt.Errorf("batch %d: failed to send %d events", i, peo.FailedEntryCount)
				return
			}
		}(i)
	}
	wg.Wait()
	if err = multierr.Combine(errors...); err != nil {
		return err
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

const (
	maxBatchSizeKB = 256 * 1024
	maxCount       = 10
)

func batch(values []types.PutEventsRequestEntry) (pages [][]types.PutEventsRequestEntry, err error) {
	var batchFrom, batchSize int
	for i, v := range values {
		size := getSize(v)
		if size > maxBatchSizeKB {
			err = fmt.Errorf("invalid PutEventRequestEntry: item with index %d is larger than the maximum allowed size of 256KB, having a size of %dKB", i, size/1024)
			return
		}
		if batchSize+size >= maxBatchSizeKB || i-batchFrom == maxCount {
			pages = append(pages, values[batchFrom:i])
			// Reset.
			batchFrom = i
			batchSize = 0
		}
		batchSize += size
	}
	if batchFrom < len(values) {
		pages = append(pages, values[batchFrom:])
	}
	return
}

func getSize(entry types.PutEventsRequestEntry) (size int) {
	if entry.Time != nil {
		size += 14
	}
	size += len(*entry.Source)
	size += len(*entry.DetailType)
	if entry.Detail != nil {
		size += len(*entry.Detail)
	}
	for _, r := range entry.Resources {
		size += len(r)
	}
	return size
}
