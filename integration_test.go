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

	err = p.Process(Add{10})
	if err == nil {
		t.Fatal("second processing without reload should have failed, but didn't")
	}

	// Reload and process again
	p, err = Load(store, id, state)
	if err != nil {
		t.Fatalf("failed to load record with id %v: %v", id, err)
	}

	dumpTable(store)

	err = p.Process(Add{10})
	if err != nil {
		t.Fatalf("second processing run failed: %v", err)
	}

	dumpTable(store)
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
