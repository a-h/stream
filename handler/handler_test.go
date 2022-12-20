package handler

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestStripDynamoDBTypes(t *testing.T) {
	var tests = []struct {
		name     string
		input    map[string]events.DynamoDBAttributeValue
		expected map[string]interface{}
	}{
		{
			name: "",
			input: map[string]events.DynamoDBAttributeValue{
				"Binary":  events.NewBinaryAttribute([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
				"Boolean": events.NewBooleanAttribute(true),
				"BinarySet": events.NewBinarySetAttribute([][]byte{
					{0xDE, 0xAD, 0xBE, 0xEF},
					{0x0D, 0x15, 0xEA, 0x5E},
				}),
				"List": events.NewListAttribute([]events.DynamoDBAttributeValue{
					events.NewStringAttribute("a"),
					events.NewNumberAttribute("1"),
				}),
				"Map": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
					"innerA": events.NewStringAttribute("value"),
					"innerB": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
						"innerInner": events.NewStringAttribute("innerInnerValue"),
					}),
				}),
				"Number (int, -)":   events.NewNumberAttribute("-1"),
				"Number (int, +)":   events.NewNumberAttribute("2000"),
				"Number (float, -)": events.NewNumberAttribute("-0.3"),
				"Number (float)":    events.NewNumberAttribute("0.0"),
				"Number (float, +)": events.NewNumberAttribute("+0.3"),
				"NumberSet":         events.NewNumberSetAttribute([]string{"0", "0.5", "1"}),
				"Null":              events.NewNullAttribute(),
				"String":            events.NewStringAttribute("string value"),
				"StringSet":         events.NewStringSetAttribute([]string{"A", "B"}),
			},
			expected: map[string]interface{}{
				"Binary":  []byte{0xDE, 0xAD, 0xBE, 0xEF},
				"Boolean": true,
				"BinarySet": [][]byte{
					{0xDE, 0xAD, 0xBE, 0xEF},
					{0x0D, 0x15, 0xEA, 0x5E},
				},
				"List": []interface{}{"a", int64(1)},
				"Map": map[string]interface{}{
					"innerA": "value",
					"innerB": map[string]interface{}{
						"innerInner": "innerInnerValue",
					},
				},
				"Number (int, -)":   int64(-1),
				"Number (int, +)":   int64(2000),
				"Number (float, -)": -0.3,
				"Number (float)":    0.0,
				"Number (float, +)": 0.3,
				"NumberSet":         []string{"0", "0.5", "1"},
				"Null":              nil,
				"String":            "string value",
				"StringSet":         []string{"A", "B"},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actual, err := stripDynamoDBTypesFromMap(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.expected, actual); diff != "" {
				t.Error(diff)
			}
		})
	}
}

type mockEventBridge struct {
	input *eventbridge.PutEventsInput
}

func (m mockEventBridge) PutEvents(_ context.Context, input *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	*m.input = *input
	return &eventbridge.PutEventsOutput{}, nil
}

func TestOnlyOutboundTypeEventsAreEmitted(t *testing.T) {
	var input eventbridge.PutEventsInput
	eventBridge = mockEventBridge{&input}
	log = zap.NewNop()
	pk := uuid.NewString()
	outbound := events.DynamoDBEventRecord{
		Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"_pk":      events.NewStringAttribute(pk),
				"_typ":     events.NewStringAttribute("CounterUpdated"),
				"_sk":      events.NewStringAttribute("OUTBOUND/CounterUpdated/1/0"),
				"newCount": events.NewNumberAttribute("1"),
				"oldCount": events.NewNumberAttribute("0"),
			},
		},
	}
	inbound := events.DynamoDBEventRecord{
		Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"_pk":    events.NewStringAttribute(pk),
				"_typ":   events.NewStringAttribute("IncrementCounter"),
				"_sk":    events.NewStringAttribute("INBOUND/IncrementCounter/1"),
				"amount": events.NewNumberAttribute("1"),
			},
		},
	}
	state := events.DynamoDBEventRecord{
		Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"_pk":   events.NewStringAttribute(pk),
				"_typ":  events.NewStringAttribute("Counter"),
				"_sk":   events.NewStringAttribute("STATE"),
				"count": events.NewNumberAttribute("1"),
			},
		},
	}
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			inbound,
			outbound,
			state,
		},
	}
	err := HandleRequest(context.Background(), event)
	if err != nil {
		t.Fatal("failed to handle request: ", err.Error())
	}
	if len(input.Entries) != 1 {
		t.Fatalf("expected 1 event entry, got %v: %#v", len(input.Entries), input.Entries)
	}
	expected := types.PutEventsRequestEntry{
		DetailType:   aws.String("CounterUpdated"),
		Detail:       aws.String(`{"newCount":1,"oldCount":0}`),
		EventBusName: aws.String(""),
		Source:       aws.String(""),
	}
	if diff := cmp.Diff(expected, input.Entries[0], cmp.AllowUnexported(types.PutEventsRequestEntry{})); diff != "" {
		t.Fatalf("unexpected event emitted: " + diff)
	}
}

func TestBatch(t *testing.T) {
	tests := []struct {
		name               string
		entries            []types.PutEventsRequestEntry
		expectedBatchSizes []int
	}{
		{
			name: "small messages are grouped to the maximum batch size of 10",
			entries: []types.PutEventsRequestEntry{
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
			},
			expectedBatchSizes: []int{10, 1},
		},
		{
			name: "big messages are separated into their own batches",
			entries: []types.PutEventsRequestEntry{
				createTestEvent(256 * 1024),
				createTestEvent(1 * 1024),
			},
			expectedBatchSizes: []int{1, 1},
		},
		{
			name: "medium messages can be grouped separated into their own batches",
			entries: []types.PutEventsRequestEntry{
				createTestEvent(128 * 1024),
				createTestEvent(64 * 1024),
				createTestEvent(64 * 1024),
				createTestEvent(64 * 1024),
			},
			expectedBatchSizes: []int{3, 1},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			batches, err := batch(test.entries)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Assert.
			actual := make([]int, len(batches))
			for i, b := range batches {
				actual[i] = len(b)
			}
			if diff := cmp.Diff(test.expectedBatchSizes, actual); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func createTestEvent(size int) types.PutEventsRequestEntry {
	detailType := "detailType"
	resource := "resource"
	source := "source"
	entry := types.PutEventsRequestEntry{
		Detail:       new(string),
		DetailType:   &detailType,
		EventBusName: aws.String("eventBusName"),
		Resources:    []string{resource},
		Source:       aws.String(source),
		Time:         nil,
		TraceHeader:  nil,
	}
	entry.Detail = aws.String(strings.Repeat("a", size-len(source)-len(resource)-len(detailType)-1))
	return entry
}

func TestBatchError(t *testing.T) {
	tests := []struct {
		name    string
		entries []types.PutEventsRequestEntry
	}{
		{
			name: "a big message results in an error",
			entries: []types.PutEventsRequestEntry{
				createTestEvent(257 * 1024),
			},
		},
		{
			name: "a big message results in an error, even if there are smaller messages too",
			entries: []types.PutEventsRequestEntry{
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(1 * 1024),
				createTestEvent(257 * 1024),
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := batch(test.entries)
			if err == nil {
				t.Fatalf("expected error not found")
			}
		})
	}
}
