package stream

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// ErrStateNotFound is returned if the state is not found.
var ErrStateNotFound = errors.New("state not found")
var ErrOptimisticConcurrency = errors.New("state has been updated since it was read, try again")

var sess *session.Session
var client *dynamodb.DynamoDB
var initErr error

func init() {
	sess, initErr = session.NewSession()
	if initErr != nil {
		return
	}
	client = dynamodb.New(sess)
}

// NewStore creates a new store using default config.
func NewStore(tableName, namespace string) (s *DynamoDBStore, err error) {
	if initErr != nil {
		err = initErr
		return
	}
	s = &DynamoDBStore{
		Client:    client,
		TableName: aws.String(tableName),
		Now: func() time.Time {
			return time.Now().UTC()
		},
		Namespace: namespace,
	}
	return
}

// NewStoreWithConfig creates a new store with custom config.
func NewStoreWithConfig(region, tableName, namespace string) (s *DynamoDBStore, err error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return
	}
	s = &DynamoDBStore{
		Client:    dynamodb.New(sess),
		TableName: aws.String(tableName),
		Now: func() time.Time {
			return time.Now().UTC()
		},
		Namespace: namespace,
	}
	return
}

// DynamoDBStore is a DynamoDB implementation of the Store interface.
type DynamoDBStore struct {
	Client    *dynamodb.DynamoDB
	TableName *string
	Now       func() time.Time
	Namespace string
}

// Get data using the id and populate the state variable.
func (ddb *DynamoDBStore) Get(id string, state State) (sequence int64, err error) {
	if reflect.ValueOf(state).Kind() != reflect.Ptr {
		err = errors.New("the state parameter must be a pointer")
		return
	}
	gio, err := ddb.Client.GetItem(&dynamodb.GetItemInput{
		TableName:      ddb.TableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"_pk": ddb.attributeValueString(ddb.createPartitionKey(id)),
			"_sk": ddb.attributeValueString(ddb.createStateRecordSortKey()),
		},
	})
	if err != nil {
		return
	}
	if len(gio.Item) == 0 {
		err = ErrStateNotFound
		return
	}
	err = dynamodbattribute.UnmarshalMap(gio.Item, state)
	if err != nil {
		return
	}
	return ddb.getRecordSequenceNumber(gio.Item)
}

// Put the updated state in the database.
func (ddb *DynamoDBStore) Put(id string, atSequence int64, state State, inbound []InboundEvent, outbound []OutboundEvent) error {
	stwi, err := ddb.createStateTransactWriteItem(id, atSequence, state)
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
	var items []*dynamodb.TransactWriteItem
	items = append(items, stwi)
	items = append(items, itwi...)
	items = append(items, otwi...)
	_, err = ddb.Client.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if strings.Contains(aerr.Message(), "ConditionalCheckFailed") {
				return ErrOptimisticConcurrency
			}
		}
	}
	return err
}

func (ddb *DynamoDBStore) createInboundTransactWriteItems(id string, atSequence int64, inbound []InboundEvent) (puts []*dynamodb.TransactWriteItem, err error) {
	puts = make([]*dynamodb.TransactWriteItem, len(inbound))
	for i := 0; i < len(inbound); i++ {
		var item map[string]*dynamodb.AttributeValue
		item, err = ddb.createRecord(id,
			ddb.createInboundRecordSortKey(inbound[i].EventName(), atSequence+int64(i+1)),
			atSequence+int64(i+1),
			inbound[i],
			inbound[i].EventName())
		if err != nil {
			return
		}
		puts[i] = ddb.createPut(item)
	}
	return
}

func (ddb *DynamoDBStore) createOutboundTransactWriteItems(id string, atSequence int64, outbound []OutboundEvent) (puts []*dynamodb.TransactWriteItem, err error) {
	puts = make([]*dynamodb.TransactWriteItem, len(outbound))
	for i := 0; i < len(outbound); i++ {
		var item map[string]*dynamodb.AttributeValue
		item, err = ddb.createRecord(id,
			ddb.createOutboundRecordSortKey(outbound[i].EventName(), atSequence+1, i),
			atSequence+int64(i+1),
			outbound[i],
			outbound[i].EventName())
		if err != nil {
			return
		}
		puts[i] = ddb.createPut(item)
	}
	return
}

func (ddb *DynamoDBStore) createPut(item map[string]*dynamodb.AttributeValue) *dynamodb.TransactWriteItem {
	return &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName:           ddb.TableName,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(#_pk)"),
			ExpressionAttributeNames: map[string]*string{
				"#_pk": aws.String("_pk"),
			},
		},
	}
}

func (ddb *DynamoDBStore) createStateTransactWriteItem(id string, atSequence int64, state State) (twi *dynamodb.TransactWriteItem, err error) {
	item, err := ddb.createRecord(id, ddb.createStateRecordSortKey(), atSequence+1, state, ddb.Namespace)
	if err != nil {
		return
	}
	twi = &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName:           ddb.TableName,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(#_pk) OR #_seq = :_seq"),
			ExpressionAttributeNames: map[string]*string{
				"#_pk":  aws.String("_pk"),
				"#_seq": aws.String("_seq"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":_seq": ddb.attributeValueInteger(int64(atSequence)),
			},
		},
	}
	return
}

func (ddb *DynamoDBStore) createPartitionKey(id string) string {
	return fmt.Sprintf(`%s/%s`, ddb.Namespace, id)
}

func (ddb *DynamoDBStore) createStateRecordSortKey() string {
	return "STATE"
}

func (ddb *DynamoDBStore) createInboundRecordSortKey(typeName string, sequence int64) string {
	return fmt.Sprintf(`INBOUND/%s/%d`, typeName, sequence)
}

func (ddb *DynamoDBStore) createOutboundRecordSortKey(typeName string, sequence int64, index int) string {
	return fmt.Sprintf(`OUTBOUND/%s/%d/%d`, typeName, sequence, index)
}

func (ddb *DynamoDBStore) attributeValueString(v string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{S: &v}
}

func (ddb *DynamoDBStore) attributeValueInteger(i int64) *dynamodb.AttributeValue {
	v := strconv.FormatInt(i, 10)
	return &dynamodb.AttributeValue{N: &v}
}

func (ddb *DynamoDBStore) createRecord(id, sk string, sequence int64, item interface{}, recordName string) (record map[string]*dynamodb.AttributeValue, err error) {
	record, err = dynamodbattribute.MarshalMap(item)
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

func (ddb *DynamoDBStore) getRecordSequenceNumber(r map[string]*dynamodb.AttributeValue) (sequence int64, err error) {
	v, ok := r["_seq"]
	if !ok {
		return sequence, fmt.Errorf("missing _seq field in record")
	}
	if v.N == nil {
		return sequence, fmt.Errorf("null _seq field in record")
	}
	sequence, err = strconv.ParseInt(*v.N, 10, 64)
	if err != nil {
		return sequence, fmt.Errorf("invalid _seq field in record: %w", err)
	}
	return
}

func (ddb *DynamoDBStore) getRecordType(r map[string]*dynamodb.AttributeValue) (typ string, err error) {
	v, ok := r["_typ"]
	if !ok {
		return "", fmt.Errorf("missing _typ field in record")
	}
	if v.S == nil {
		return "", fmt.Errorf("null _typ field in record")
	}
	return *v.S, nil
}

func NewInboundEventReader() *InboundEventReader {
	return &InboundEventReader{
		readers: make(map[string]func(item map[string]*dynamodb.AttributeValue) (InboundEvent, error), 0),
	}
}

type InboundEventReader struct {
	readers map[string]func(item map[string]*dynamodb.AttributeValue) (InboundEvent, error)
}

func (r *InboundEventReader) Add(eventName string, f func(item map[string]*dynamodb.AttributeValue) (InboundEvent, error)) {
	r.readers[eventName] = f
}

func (r *InboundEventReader) Read(eventName string, item map[string]*dynamodb.AttributeValue) (e InboundEvent, ok bool, err error) {
	f, ok := r.readers[eventName]
	if !ok {
		return
	}
	e, err = f(item)
	return
}

func NewOutboundEventReader() *OutboundEventReader {
	return &OutboundEventReader{
		readers: make(map[string]func(item map[string]*dynamodb.AttributeValue) (OutboundEvent, error), 0),
	}
}

type OutboundEventReader struct {
	readers map[string]func(item map[string]*dynamodb.AttributeValue) (OutboundEvent, error)
}

func (r *OutboundEventReader) Add(eventName string, f func(item map[string]*dynamodb.AttributeValue) (OutboundEvent, error)) {
	r.readers[eventName] = f
}

func (r *OutboundEventReader) Read(eventName string, item map[string]*dynamodb.AttributeValue) (e OutboundEvent, ok bool, err error) {
	f, ok := r.readers[eventName]
	if !ok {
		return
	}
	e, err = f(item)
	return
}

// Query data for the id.
func (ddb *DynamoDBStore) Query(id string, state State, inboundEventReader *InboundEventReader, outboundEventReader *OutboundEventReader) (sequence int64, inbound []InboundEvent, outbound []OutboundEvent, err error) {
	if reflect.ValueOf(state).Kind() != reflect.Ptr {
		err = errors.New("the state parameter must be a pointer")
		return
	}
	qi := &dynamodb.QueryInput{
		TableName:              ddb.TableName,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("#_pk = :_pk"),
		ExpressionAttributeNames: map[string]*string{
			"#_pk": aws.String("_pk"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":_pk": ddb.attributeValueString(ddb.createPartitionKey(id)),
		},
	}
	var found bool
	var pagerError error
	pager := func(qo *dynamodb.QueryOutput, lastPage bool) (carryOn bool) {
		for i := 0; i < len(qo.Items); i++ {
			r := qo.Items[i]
			switch ddb.getSortKeyPrefix(r) {
			case "STATE":
				found = true
				pagerError = dynamodbattribute.UnmarshalMap(r, state)
				if pagerError != nil {
					return false
				}
				sequence, pagerError = ddb.getRecordSequenceNumber(r)
				if pagerError != nil {
					return false
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
	err = ddb.Client.QueryPages(qi, pager)
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

func (ddb *DynamoDBStore) getSortKeyPrefix(item map[string]*dynamodb.AttributeValue) (prefix string) {
	sk, ok := item["_sk"]
	if !ok || sk.S == nil {
		return ""
	}
	return strings.SplitN(*sk.S, "/", 2)[0]
}
