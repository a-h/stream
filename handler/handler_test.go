package handler

import (
	"context"
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
