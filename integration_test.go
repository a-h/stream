package stream

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
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
		break
	case Subtract:
		s.Count--
		s.Sum -= e.Number
		s.Value = float64(s.Sum) / float64(s.Count)
		break
	}
	outbound = append(outbound, Average{s.Value}, Sum(s.Sum))
	return
}

type Add struct {
	Number int
}

func (ai Add) EventName() string { return "Add" }
func (ai Add) IsInbound()        {}

type Subtract struct {
	Number int
}

func (ai Subtract) EventName() string { return "Subtract" }
func (ai Subtract) IsInbound()        {}

type Average struct {
	Value float64
}

func (ai Average) EventName() string { return "Average" }
func (ai Average) IsOutbound()       {}

type Sum int

func (ai Sum) EventName() string { return "Sum" }
func (ai Sum) IsOutbound()       {}

// BUG: Multiples of the same INBOUND event type breaks future writes
// because the sort key is not namespaced by STATE's sequence number,
// but uses this number as a base to start incrementing from.
//
// It also increments _seq for each event. This does not cause writes
// to fail but it does mean that the INBOUND/OUTBOUND events are not
// associated with the state transition in which they were
// consumed/produced.
//
// Consider this code:
//	state := &AverageState{}
//      p, _ := stream.New(store, "<id>", state)
// 	_ = p.Process(Add{10}, Add{15}, Add{20})
//
// This creates items with the following sort keys and sequence numbers:
// 	| no:  | _pk:         | _sk:          | _seq:  | new/updated: |
//	|------|--------------|---------------|--------|--------------|
//	|    1 | Average/<id> | STATE         |      1 | *            |
//	|    2 | Average/<id> | INBOUND/Add/1 |      1 | *            |
//	|    3 | Average/<id> | INBOUND/Add/2 |      2 | *            |
//	|    4 | Average/<id> | INBOUND/Add/3 |      3 | *            |
//
// A future write occurs e.g.:
//	state := &AverageState{}
//      p, _ := stream.Load(store, "<id>", state)
// 	_ = p.Process(Add{10})
//
// This attempts to update the table in this way
// 	| no:  | _pk:         | _sk:          | _seq:  | new/updated: |
//	|------|--------------|---------------|--------|--------------|
//	|    1 | Average/<id> | STATE         |      2 | *            |
//	|    2 | Average/<id> | INBOUND/Add/1 |      1 |              |
//	|    3 | Average/<id> | INBOUND/Add/2 |      2 |              |
//	|    4 | Average/<id> | INBOUND/Add/3 |      3 |              |
//	|    5 | Average/<id> | INBOUND/Add/2 |      2 | *            |
//
// Items 5 ends up clashing with item 2 which fails the condition check and the
// write transaction fails.
//
// IMO, the event sort keys should be of the form:
// 	`<EventType>/<STATE-_seq>/<EventIndex>/<EventName>`
//
// Resulting in the following behaviour
//
// First write:
// 	| no:  | _pk:         | _sk:            | _seq:  | new/updated: |
//	|------|--------------|-----------------|--------|--------------|
//	|    1 | Average/<id> | STATE           |      1 | *            |
//	|    2 | Average/<id> | INBOUND/1/1/Add |      1 | *            |
//	|    3 | Average/<id> | INBOUND/1/2/Add |      1 | *            |
//	|    4 | Average/<id> | INBOUND/1/3/Add |      1 | *            |
//
// Second write:
// 	| no:  | _pk:         | _sk:            | _seq:  | new/updated: |
//	|------|--------------|-----------------|--------|--------------|
//	|    1 | Average/<id> | STATE           |      2 | *            |
//	|    2 | Average/<id> | INBOUND/1/1/Add |      1 |              |
//	|    3 | Average/<id> | INBOUND/1/2/Add |      1 |              |
//	|    4 | Average/<id> | INBOUND/1/3/Add |      1 |              |
//	|    5 | Average/<id> | INBOUND/2/1/Add |      2 | *            |
//
// This does make _seq for the events redundant, as the sort key maintains the
// ordering itself, but I don't see any harm in that. We should probably update
// the OUTBOUND events to be in the same format as well, for consistency.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	name := createLocalTable(t)
	t.Cleanup(func() { deleteLocalTable(t, name) })

	state := &AverageState{}
	store, err := NewStore(name, "Average")
	store.Client = testClient
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	id := uuid.NewString()
	p, err := New(store, id, state)
	if err != nil {
		t.Fatalf("failed to create stream processor: %v", err)
	}

	// Process multiple events.
	err = p.Process(Add{10}, Add{15}, Subtract{7})
	if err != nil {
		t.Fatalf("first processing run failed: %v", err)
	}

	// Reload and process again
	p, err = Load(store, id, state)
	if err != nil {
		t.Fatalf("failed to load record with id %v: %v", id, err)
	}

	// BUG: This can never succeed due the aforementioned bug
	err = p.Process(Add{10})
	if err != nil {
		t.Fatalf("second processing run failed: %v", err)
	}
}

func dumpTable(store *DynamoDBStore) {
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
		for _, item := range page.Items {
			items = append(items, item)
		}
	}
	_ = json.NewEncoder(os.Stderr).Encode(items)
}
