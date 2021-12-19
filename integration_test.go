package stream

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
)

type AverageState struct {
	Sum   int
	Count int
	Value float64
}

func (s *AverageState) Process(event InboundEvent) (outbound []OutboundEvent, err error) {
	switch e := event.(type) {
	case Add:
		s.Count++
		s.Sum += e.Number
		s.Value = float64(s.Sum) / float64(s.Count)
	case Subtract:
		s.Count++
		s.Sum -= e.Number
		s.Value = float64(s.Sum) / float64(s.Count)
	}
	outbound = append(outbound, Average{s.Value}, Count{s.Count})
	return
}

type Add struct {
	Number int
}

func (Add) EventName() string { return "Add" }
func (Add) IsInbound()        {}

type Subtract struct {
	Number int
}

func (Subtract) EventName() string { return "Subtract" }
func (Subtract) IsInbound()        {}

type Average struct {
	Value float64
}

func (Average) EventName() string { return "Average" }
func (Average) IsOutbound()       {}

type Count struct {
	Number int
}

func (Count) EventName() string { return "Count" }
func (Count) IsOutbound()       {}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	name := createLocalTable(t)
	t.Cleanup(func() { deleteLocalTable(t, name) })

	// Create store.
	store, err := NewStore(name, "Average", WithPersistStateHistory(true))
	store.Client = testClient
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Create new state record.
	id := "id"
	state := &AverageState{}
	p, err := New(store, id, state)
	if err != nil {
		t.Fatalf("failed to create stream processor: %v", err)
	}

	// Process multiple events.
	err = p.Process(Add{2}, Add{3}, Subtract{1})
	if err != nil {
		t.Fatalf("first processing run failed: %v", err)
	}
	err = p.Process(Add{1})
	if err == nil {
		t.Fatal("second processing without reload should have failed, but didn't")
	}

	// Reload and process again.
	p, err = Load(store, id, state)
	if err != nil {
		t.Fatalf("failed to load record with id %v: %v", id, err)
	}
	err = p.Process(Add{20})
	if err != nil {
		t.Fatalf("second processing run failed: %v", err)
	}

	// Reload and process one last time.
	p, err = Load(store, id, state)
	if err != nil {
		t.Fatalf("failed to load record with id %v: %v", id, err)
	}
	err = p.Process(Add{18})
	if err != nil {
		t.Fatalf("second processing run failed: %v", err)
	}

	// Ensure Query works as expected
	inboundEventReader := NewInboundEventReader().
		Add("Add", func(item map[string]types.AttributeValue) (e InboundEvent, err error) {
			e = &Add{}
			err = attributevalue.UnmarshalMap(item, e)
			return
		}).
		Add("Subtract", func(item map[string]types.AttributeValue) (e InboundEvent, err error) {
			e = &Subtract{}
			err = attributevalue.UnmarshalMap(item, e)
			return
		})

	outboundEventReader := NewOutboundEventReader().
		Add("Average", func(item map[string]types.AttributeValue) (e OutboundEvent, err error) {
			e = &Average{}
			err = attributevalue.UnmarshalMap(item, e)
			return
		}).
		Add("Count", func(item map[string]types.AttributeValue) (e OutboundEvent, err error) {
			e = &Count{}
			err = attributevalue.UnmarshalMap(item, e)
			return
		})
	stateHistoryReader := NewStateHistoryReader(func(item map[string]types.AttributeValue) (s State, err error) {
		s = &AverageState{}
		err = attributevalue.UnmarshalMap(item, &s)
		return
	})
	expectedInboundEvents := []InboundEvent{&Add{2}, &Add{3}, &Subtract{1}, &Add{20}, &Add{18}}
	expectedOutboundEvents := []OutboundEvent{
		&Average{2}, &Count{1},
		&Average{2.5}, &Count{2},
		&Average{float64(4) / 3}, &Count{3},
		&Average{6}, &Count{4},
		&Average{8.4}, &Count{5},
	}
	expectedStateHistory := []State{
		&AverageState{Sum: 4, Count: 3, Value: float64(4) / 3},
		&AverageState{Sum: 24, Count: 4, Value: 6},
		state,
	}
	seq, inboundEvents, outboundEvents, stateHistory, err := store.QueryWithHistory(id, state, inboundEventReader, outboundEventReader, stateHistoryReader)
	if err != nil {
		t.Fatalf("failed to query store: %v", err)
	}
	if seq != 3 {
		t.Fatalf("expected seq to be 3, but got %v", seq)
	}
	if diff := cmp.Diff(expectedInboundEvents, inboundEvents); diff != "" {
		t.Error("unexpected inbound events")
		t.Error(diff)
	}
	if diff := cmp.Diff(expectedOutboundEvents, outboundEvents); diff != "" {
		t.Error("unexpected outbound events")
		t.Error(diff)
	}
	if diff := cmp.Diff(expectedStateHistory, stateHistory); diff != "" {
		t.Error("unexpected state history")
		t.Error(diff)
	}

	// Ensure database state is as expected
	expectedItems := []map[string]types.AttributeValue{
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "INBOUND/1/0/Add"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "INBOUND/1/1/Add"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "INBOUND/1/2/Subtract"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "2"}, "_sk": &types.AttributeValueMemberS{Value: "INBOUND/2/0/Add"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "3"}, "_sk": &types.AttributeValueMemberS{Value: "INBOUND/3/0/Add"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/1/0/Average"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/1/1/Count"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/1/2/Average"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/1/3/Count"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/1/4/Average"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/1/5/Count"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "2"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/2/0/Average"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "2"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/2/1/Count"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "3"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/3/0/Average"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "3"}, "_sk": &types.AttributeValueMemberS{Value: "OUTBOUND/3/1/Count"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "3"}, "_sk": &types.AttributeValueMemberS{Value: "STATE"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "1"}, "_sk": &types.AttributeValueMemberS{Value: "STATE/1"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "2"}, "_sk": &types.AttributeValueMemberS{Value: "STATE/2"}},
		{"_pk": &types.AttributeValueMemberS{Value: "Average/id"}, "_seq": &types.AttributeValueMemberN{Value: "3"}, "_sk": &types.AttributeValueMemberS{Value: "STATE/3"}},
	}
	input := &dynamodb.ScanInput{
		TableName:            store.TableName,
		Select:               types.SelectSpecificAttributes,
		ProjectionExpression: aws.String("#_pk, #_sk, #_seq"),
		ExpressionAttributeNames: map[string]string{
			"#_pk":  "_pk",
			"#_sk":  "_sk",
			"#_seq": "_seq",
		},
	}
	var items []map[string]types.AttributeValue
	pages := dynamodb.NewScanPaginator(store.Client, input)
	for pages.HasMorePages() {
		page, err := pages.NextPage(context.Background())
		if err != nil {
			panic(err)
		}
		items = append(items, page.Items...)
	}
	if diff := cmp.Diff(expectedItems, items, cmp.AllowUnexported(types.AttributeValueMemberS{}, types.AttributeValueMemberN{})); diff != "" {
		t.Error("unexpected database state")
		t.Error(diff)
	}
}
