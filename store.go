package stream

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrStateNotFound is returned if the state is not found.
var ErrStateNotFound = errors.New("state not found")
var ErrOptimisticConcurrency = errors.New("state has been updated since it was read, try again")

type StoreOption func(*StoreOptions) error

type StoreOptions struct {
	Region              string
	Client              *dynamodb.Client
	PersistStateHistory bool
}

func WithRegion(region string) StoreOption {
	return func(o *StoreOptions) error {
		o.Region = region
		return nil
	}
}

func WithPersistStateHistory(do bool) StoreOption {
	return func(o *StoreOptions) error {
		o.PersistStateHistory = do
		return nil
	}
}

func WithClient(client *dynamodb.Client) StoreOption {
	return func(o *StoreOptions) error {
		o.Client = client
		return nil
	}
}

// NewStore creates a new store using default config.
func NewStore(tableName, namespace string, opts ...StoreOption) (s *DynamoDBStore, err error) {
	o := StoreOptions{}
	for _, opt := range opts {
		err = opt(&o)
		if err != nil {
			return
		}
	}
	if o.Client == nil {
		var cfg aws.Config
		cfg, err = config.LoadDefaultConfig(context.Background(), config.WithRegion(o.Region))
		if err != nil {
			return
		}
		o.Client = dynamodb.NewFromConfig(cfg)
	}
	s = &DynamoDBStore{
		Client:              o.Client,
		TableName:           aws.String(tableName),
		Namespace:           namespace,
		PersistStateHistory: o.PersistStateHistory,
		Now: func() time.Time {
			return time.Now().UTC()
		},
	}
	return
}

// NewStoreWithConfig creates a new store with custom config.
//
// Deprecated: Use NewStore with the WithRegion option
func NewStoreWithConfig(region, tableName, namespace string, opts ...StoreOption) (s *DynamoDBStore, err error) {
	opts = append(opts, WithRegion(region))
	return NewStore(tableName, namespace, opts...)
}

// DynamoDBStore is a DynamoDB implementation of the Store interface.
type DynamoDBStore struct {
	Client              *dynamodb.Client
	TableName           *string
	Namespace           string
	PersistStateHistory bool
	Now                 func() time.Time
}

// Get data using the id and populate the state variable.
func (ddb *DynamoDBStore) Get(id string, state State) (sequence int64, err error) {
	if reflect.ValueOf(state).Kind() != reflect.Ptr {
		err = errors.New("the state parameter must be a pointer")
		return
	}
	gio, err := ddb.Client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      ddb.TableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"_pk": &types.AttributeValueMemberS{Value: ddb.createPartitionKey(id)},
			"_sk": &types.AttributeValueMemberS{Value: ddb.createStateRecordSortKey()},
		},
	})
	if err != nil {
		return
	}
	if len(gio.Item) == 0 {
		err = ErrStateNotFound
		return
	}
	err = attributevalue.UnmarshalMap(gio.Item, state)
	if err != nil {
		return
	}
	return ddb.getRecordSequenceNumber(gio.Item)
}

// Put the updated state in the database.
func (ddb *DynamoDBStore) Put(id string, atSequence int64, state State, inbound []InboundEvent, outbound []OutboundEvent) error {
	atSequence++
	stwi, err := ddb.createStateTransactWriteItems(id, atSequence, state)
	if err != nil {
		return err
	}
	itwi, err := ddb.createInboundTransactWriteItems(id, atSequence, inbound)
	if err != nil {
		return err
	}
	otwi, err := ddb.createOutboundTransactWriteItems(id, atSequence, outbound)
	if err != nil {
		return err
	}
	var items []types.TransactWriteItem
	items = append(items, stwi...)
	items = append(items, itwi...)
	items = append(items, otwi...)
	_, err = ddb.Client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		var transactionCanceled *types.TransactionCanceledException
		if errors.As(err, &transactionCanceled) {
			for _, reason := range transactionCanceled.CancellationReasons {
				if *reason.Code == "ConditionalCheckFailed" {
					return ErrOptimisticConcurrency
				}
			}
		}
	}
	return err
}

func (ddb *DynamoDBStore) createInboundTransactWriteItems(id string, atSequence int64, inbound []InboundEvent) (puts []types.TransactWriteItem, err error) {
	puts = make([]types.TransactWriteItem, len(inbound))
	for i := 0; i < len(inbound); i++ {
		var item map[string]types.AttributeValue
		item, err = ddb.createRecord(id,
			ddb.createInboundRecordSortKey(inbound[i].EventName(), atSequence, i),
			atSequence,
			inbound[i],
			inbound[i].EventName())
		if err != nil {
			return
		}
		puts[i] = ddb.createPut(item)
	}
	return
}

func (ddb *DynamoDBStore) createOutboundTransactWriteItems(id string, atSequence int64, outbound []OutboundEvent) (puts []types.TransactWriteItem, err error) {
	puts = make([]types.TransactWriteItem, len(outbound))
	for i := 0; i < len(outbound); i++ {
		var item map[string]types.AttributeValue
		item, err = ddb.createRecord(id,
			ddb.createOutboundRecordSortKey(outbound[i].EventName(), atSequence, i),
			atSequence,
			outbound[i],
			outbound[i].EventName())
		if err != nil {
			return
		}
		puts[i] = ddb.createPut(item)
	}
	return
}

func (ddb *DynamoDBStore) createPut(item map[string]types.AttributeValue) types.TransactWriteItem {
	return types.TransactWriteItem{
		Put: &types.Put{
			TableName:           ddb.TableName,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(#_pk)"),
			ExpressionAttributeNames: map[string]string{
				"#_pk": "_pk",
			},
		},
	}
}

func (ddb *DynamoDBStore) createStateTransactWriteItem(id string, atSequence int64, state State, key string) (twi types.TransactWriteItem, err error) {
	item, err := ddb.createRecord(id, key, atSequence, state, ddb.Namespace)
	if err != nil {
		return
	}
	twi = types.TransactWriteItem{
		Put: &types.Put{
			TableName:           ddb.TableName,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(#_pk) OR #_seq = :_seq"),
			ExpressionAttributeNames: map[string]string{
				"#_pk":  "_pk",
				"#_seq": "_seq",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":_seq": ddb.attributeValueInteger(int64(atSequence - 1)),
			},
		},
	}
	return
}

func (ddb *DynamoDBStore) createStateTransactWriteItems(id string, atSequence int64, state State) (twis []types.TransactWriteItem, err error) {
	twi, err := ddb.createStateTransactWriteItem(id, atSequence, state, ddb.createStateRecordSortKey())
	if err != nil {
		return
	}
	twis = append(twis, twi)
	if ddb.PersistStateHistory {
		twi, err = ddb.createStateTransactWriteItem(id, atSequence, state, ddb.createVersionedRecordSortKey(atSequence))
		if err != nil {
			return
		}
		twis = append(twis, twi)
	}
	return
}

func (ddb *DynamoDBStore) createPartitionKey(id string) string {
	return fmt.Sprintf(`%s/%s`, ddb.Namespace, id)
}

func (ddb *DynamoDBStore) createStateRecordSortKey() string {
	return "STATE"
}

func (ddb *DynamoDBStore) createVersionedRecordSortKey(atSequence int64) string {
	return fmt.Sprintf("STATE/%d", atSequence)
}

func (ddb *DynamoDBStore) createInboundRecordSortKey(typeName string, sequence int64, index int) string {
	return fmt.Sprintf(`INBOUND/%d/%d/%s`, sequence, index, typeName)
}

func (ddb *DynamoDBStore) createOutboundRecordSortKey(typeName string, sequence int64, index int) string {
	return fmt.Sprintf(`OUTBOUND/%d/%d/%s`, sequence, index, typeName)
}

func (ddb *DynamoDBStore) attributeValueString(v string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: v}
}

func (ddb *DynamoDBStore) attributeValueInteger(i int64) types.AttributeValue {
	v := strconv.FormatInt(i, 10)
	return &types.AttributeValueMemberN{Value: v}
}

func (ddb *DynamoDBStore) createRecord(id, sk string, sequence int64, item interface{}, recordName string) (record map[string]types.AttributeValue, err error) {
	record, err = attributevalue.MarshalMap(item)
	if err != nil {
		err = fmt.Errorf("error marshalling item to map: %w", err)
		return
	}
	record["_namespace"] = ddb.attributeValueString(ddb.Namespace)
	record["_pk"] = ddb.attributeValueString(ddb.createPartitionKey(id))
	record["_seq"] = ddb.attributeValueInteger(int64(sequence))
	record["_sk"] = ddb.attributeValueString(sk)
	record["_typ"] = ddb.attributeValueString(recordName)
	record["_ts"] = ddb.attributeValueInteger(ddb.Now().Unix())
	record["_date"] = ddb.attributeValueString(ddb.Now().Format(time.RFC3339))
	return
}

func (ddb *DynamoDBStore) getRecordSequenceNumber(r map[string]types.AttributeValue) (sequence int64, err error) {
	a, ok := r["_seq"]
	if !ok {
		return sequence, fmt.Errorf("missing _seq field in record")
	}
	v, ok := a.(*types.AttributeValueMemberN)
	if !ok {
		return sequence, fmt.Errorf("null _seq field in record")
	}
	sequence, err = strconv.ParseInt(v.Value, 10, 64)
	if err != nil {
		return sequence, fmt.Errorf("invalid _seq field in record: %w", err)
	}
	return
}

func (ddb *DynamoDBStore) getRecordType(r map[string]types.AttributeValue) (typ string, err error) {
	a, ok := r["_typ"]
	if !ok {
		return "", fmt.Errorf("missing _typ field in record")
	}
	v, ok := a.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("null _typ field in record")
	}
	return v.Value, nil
}

func NewInboundEventReader() *InboundEventReader {
	return &InboundEventReader{
		readers: make(map[string]func(item map[string]types.AttributeValue) (InboundEvent, error), 0),
	}
}

type InboundEventReader struct {
	readers map[string]func(item map[string]types.AttributeValue) (InboundEvent, error)
}

func (r *InboundEventReader) Add(eventName string, f func(item map[string]types.AttributeValue) (InboundEvent, error)) *InboundEventReader {
	r.readers[eventName] = f
	return r
}

func (r *InboundEventReader) Read(eventName string, item map[string]types.AttributeValue) (e InboundEvent, ok bool, err error) {
	f, ok := r.readers[eventName]
	if !ok {
		return
	}
	e, err = f(item)
	return
}

func NewOutboundEventReader() *OutboundEventReader {
	return &OutboundEventReader{
		readers: make(map[string]func(item map[string]types.AttributeValue) (OutboundEvent, error), 0),
	}
}

type OutboundEventReader struct {
	readers map[string]func(item map[string]types.AttributeValue) (OutboundEvent, error)
}

func (r *OutboundEventReader) Add(eventName string, f func(item map[string]types.AttributeValue) (OutboundEvent, error)) *OutboundEventReader {
	r.readers[eventName] = f
	return r
}

func (r *OutboundEventReader) Read(eventName string, item map[string]types.AttributeValue) (e OutboundEvent, ok bool, err error) {
	f, ok := r.readers[eventName]
	if !ok {
		return
	}
	e, err = f(item)
	return
}

func NewStateHistoryReader(f func(item map[string]types.AttributeValue) (State, error)) *StateHistoryReader {
	return &StateHistoryReader{reader: f}
}

type StateHistoryReader struct {
	reader func(item map[string]types.AttributeValue) (State, error)
}

func (r *StateHistoryReader) Read(item map[string]types.AttributeValue) (e State, err error) {
	e, err = r.reader(item)
	return
}

func (ddb *DynamoDBStore) queryPages(qi *dynamodb.QueryInput, pager func(*dynamodb.QueryOutput, bool) bool) (err error) {
	pages := dynamodb.NewQueryPaginator(ddb.Client, qi)
	for carryOn := pages.HasMorePages(); carryOn && pages.HasMorePages(); {
		var page *dynamodb.QueryOutput
		page, err = pages.NextPage(context.Background())
		if err != nil {
			return err
		}
		carryOn = pager(page, pages.HasMorePages())
	}
	return
}

func (ddb *DynamoDBStore) Query(id string, state State, inboundEventReader *InboundEventReader, outboundEventReader *OutboundEventReader) (sequence int64, inbound []InboundEvent, outbound []OutboundEvent, err error) {
	noopStateHistoryReader := NewStateHistoryReader(func(item map[string]types.AttributeValue) (State, error) { return nil, nil })
	sequence, inbound, outbound, _, err = ddb.QueryWithHistory(id, state, inboundEventReader, outboundEventReader, noopStateHistoryReader)
	return
}

// Query data for the id.
func (ddb *DynamoDBStore) QueryWithHistory(id string, state State, inboundEventReader *InboundEventReader, outboundEventReader *OutboundEventReader, stateHistoryReader *StateHistoryReader) (sequence int64, inbound []InboundEvent, outbound []OutboundEvent, stateHistory []State, err error) {
	if reflect.ValueOf(state).Kind() != reflect.Ptr {
		err = errors.New("the state parameter must be a pointer")
		return
	}
	qi := &dynamodb.QueryInput{
		TableName:              ddb.TableName,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("#_pk = :_pk"),
		ExpressionAttributeNames: map[string]string{
			"#_pk": "_pk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":_pk": ddb.attributeValueString(ddb.createPartitionKey(id)),
		},
	}
	var found bool
	var pagerError error
	pager := func(qo *dynamodb.QueryOutput, _ bool) (carryOn bool) {
		for i := 0; i < len(qo.Items); i++ {
			r := qo.Items[i]
			prefix, suffix := ddb.splitSortKey(r)
			switch prefix {
			case "STATE":
				if suffix == "" {
					found = true
					pagerError = attributevalue.UnmarshalMap(r, state)
					if pagerError != nil {
						return false
					}
					sequence, pagerError = ddb.getRecordSequenceNumber(r)
					if pagerError != nil {
						return false
					}
				} else {
					transition, err := stateHistoryReader.Read(r)
					if err != nil {
						pagerError = err
						return false
					}
					stateHistory = append(stateHistory, transition)
				}
			case "INBOUND":
				var typ string
				typ, pagerError = ddb.getRecordType(r)
				if pagerError != nil {
					return false
				}
				event, ok, err := inboundEventReader.Read(typ, r)
				if err != nil {
					pagerError = err
					return false
				}
				if !ok {
					pagerError = fmt.Errorf("inbound event: no reader for %q", typ)
					return false
				}
				inbound = append(inbound, event)
			case "OUTBOUND":
				var typ string
				typ, pagerError = ddb.getRecordType(r)
				if pagerError != nil {
					return false
				}
				event, ok, err := outboundEventReader.Read(typ, r)
				if err != nil {
					pagerError = err
					return false
				}
				if !ok {
					pagerError = fmt.Errorf("outbound event: no reader for %q", typ)
					return false
				}
				outbound = append(outbound, event)
			}
		}
		return true
	}
	err = ddb.queryPages(qi, pager)
	if err != nil {
		return
	}
	if pagerError != nil {
		err = pagerError
		return
	}
	if !found {
		err = ErrStateNotFound
		return
	}
	return
}

func (ddb *DynamoDBStore) splitSortKey(item map[string]types.AttributeValue) (prefix string, suffix string) {
	sk, ok := item["_sk"]
	if !ok {
		return
	}
	v, ok := sk.(*types.AttributeValueMemberS)
	if ok {
		split := strings.SplitN(v.Value, "/", 2)
		prefix = split[0]
		if len(split) == 2 {
			suffix = split[1]
		}
	}
	return
}
