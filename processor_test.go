package stream

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/go-cmp/cmp"
)

// BatchState outputs a BatchOutput when BatchSize BatchInputs have been received.
type BatchState struct {
	BatchSize      int
	BatchesEmitted int
	Values         []int
}

func (s *BatchState) Process(event InboundEvent) (outbound []OutboundEvent) {
	switch e := event.(type) {
	case BatchInput:
		s.Values = append(s.Values, e.Number)
		if len(s.Values) >= s.BatchSize {
			outbound = append(outbound, BatchOutput{Numbers: s.Values})
			s.BatchesEmitted++
			s.Values = nil
		}
		break
	}
	return
}

type BatchInput struct {
	Number int
}

func (bi BatchInput) EventName() string { return "BatchInput" }
func (bi BatchInput) IsInbound()        {}

type BatchOutput struct {
	Numbers []int
}

func (bo BatchOutput) EventName() string { return "BatchOutput" }
func (bo BatchOutput) IsOutbound()       {}

// Logic can be tested without requiring an integration test.

func TestBatch(t *testing.T) {
	var tests = []struct {
		name                  string
		initial               *BatchState
		events                []InboundEvent
		expected              *BatchState
		expectedOuboundEvents []OutboundEvent
	}{
		{
			name: "values are added to the state",
			initial: &BatchState{
				BatchSize: 100,
			},
			events: []InboundEvent{
				BatchInput{Number: 1},
				BatchInput{Number: 2},
				BatchInput{Number: 3},
			},
			expected: &BatchState{
				BatchSize: 100,
				Values:    []int{1, 2, 3},
			},
			expectedOuboundEvents: nil,
		},
		{
			name: "the values state is cleared after events are emitted",
			initial: &BatchState{
				BatchSize: 2,
			},
			events: []InboundEvent{
				BatchInput{Number: 1},
				BatchInput{Number: 2},
			},
			expected: &BatchState{
				BatchSize:      2,
				BatchesEmitted: 1,
			},
			expectedOuboundEvents: []OutboundEvent{
				BatchOutput{Numbers: []int{1, 2}},
			},
		},
		{
			name: "multiple batches can be emitted",
			initial: &BatchState{
				BatchSize: 2,
			},
			events: []InboundEvent{
				BatchInput{Number: 1},
				BatchInput{Number: 2},
				BatchInput{Number: 3},
				BatchInput{Number: 4},
				BatchInput{Number: 5},
				BatchInput{Number: 6},
				BatchInput{Number: 7},
			},
			expected: &BatchState{
				BatchSize:      2,
				BatchesEmitted: 3,
				Values:         []int{7},
			},
			expectedOuboundEvents: []OutboundEvent{
				BatchOutput{Numbers: []int{1, 2}},
				BatchOutput{Numbers: []int{3, 4}},
				BatchOutput{Numbers: []int{5, 6}},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Arrange.
			actual := tt.initial
			var actualOutboundEvents []OutboundEvent

			// Act.
			for i := 0; i < len(tt.events); i++ {
				actualOutboundEvents = append(actualOutboundEvents, actual.Process(tt.events[i])...)
			}

			// Assert.
			if diff := cmp.Diff(tt.expected, actual); diff != "" {
				t.Error("unexpected state")
				t.Error(diff)
			}
			if diff := cmp.Diff(tt.expectedOuboundEvents, actualOutboundEvents); diff != "" {
				t.Error("unexpected outbound events")
				t.Error(diff)
			}
		})
	}
}

func TestProcessorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(region, name, "Batch")
	s.Client = testClient
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Create an empty state record.
	state := &BatchState{
		BatchSize:      2,
		BatchesEmitted: 0,
	}
	processor, err := New(s, "id", state)
	if err != nil {
		t.Fatalf("failed to create new state: %v", err)
	}

	// Process 4 events.
	err = processor.Process(BatchInput{Number: 1},
		BatchInput{Number: 2},
		BatchInput{Number: 3},
		BatchInput{Number: 4},
	)
	if err != nil {
		t.Errorf("failed to process events: %v", err)
	}

	// Expect the expected state to match.
	expected := &BatchState{
		BatchSize:      2,
		BatchesEmitted: 2,
	}
	if diff := cmp.Diff(expected, state); diff != "" {
		t.Error("unexpected state")
		t.Error(diff)
	}

	// Get the data fresh from the DB.
	fresh := &BatchState{}
	_, err = Load(s, "id", fresh)
	if err != nil {
		t.Fatalf("failed to load data: %v", err)
	}
	if diff := cmp.Diff(expected, fresh); diff != "" {
		t.Error("unexpected state after load")
		t.Error(diff)
	}

	// Query to check the state of the database.
	queriedState := &BatchState{}
	inboundEventReader := NewInboundEventReader()
	inboundEventReader.Add(BatchInput{}.EventName(), func(item map[string]*dynamodb.AttributeValue) (InboundEvent, error) {
		var event BatchInput
		err := dynamodbattribute.UnmarshalMap(item, &event)
		return event, err
	})
	outboundEventReader := NewOutboundEventReader()
	outboundEventReader.Add(BatchOutput{}.EventName(), func(item map[string]*dynamodb.AttributeValue) (OutboundEvent, error) {
		var event BatchOutput
		err := dynamodbattribute.UnmarshalMap(item, &event)
		return event, err
	})
	sequence, inbound, outbound, err := s.Query("id", queriedState, inboundEventReader, outboundEventReader)
	if sequence != 1 {
		t.Errorf("query expected sequence of 1, got %d", sequence)
	}
	if len(inbound) != 4 {
		t.Errorf("query expected 4 inbound records to be stored, got %d", len(inbound))
	}
	if len(outbound) != 2 {
		t.Errorf("query expected 2 outbound records to be stored, got %d", len(outbound))
	}
	if diff := cmp.Diff(expected, queriedState); diff != "" {
		t.Error("unexpected state after query")
		t.Error(diff)
	}
}
