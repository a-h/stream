package handler

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/go-cmp/cmp"
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
