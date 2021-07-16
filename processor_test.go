package stream

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/go-cmp/cmp"
)

func NewBatchState() *BatchState {
	return &BatchState{
		BatchSize: 2,
	}
}

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
	state := NewBatchState()
	processor, err := New(s, "id", state)
	if err != nil {
		t.Fatalf("failed to create new state: %v", err)
	}

	t.Run("processing inbound events updates the state", func(t *testing.T) {
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
	})

	t.Run("load returns the state without needing to process all the inbound events", func(t *testing.T) {
		fresh := &BatchState{}
		_, err = Load(s, "id", fresh)
		if err != nil {
			t.Fatalf("failed to load data: %v", err)
		}
		expected := &BatchState{
			BatchSize:      2,
			BatchesEmitted: 2,
		}
		if diff := cmp.Diff(expected, fresh); diff != "" {
			t.Error("unexpected state after load")
			t.Error(diff)
		}
	})

	queriedState := &BatchState{}
	var queriedSequence int64
	var queriedInbound []InboundEvent
	var queriedOutbound []OutboundEvent
	t.Run("it is possible to query the state, inbound and outbound events", func(t *testing.T) {
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
		queriedSequence, queriedInbound, queriedOutbound, err = s.Query("id", queriedState, inboundEventReader, outboundEventReader)
		if queriedSequence != 1 {
			t.Errorf("query expected sequence of 1, got %d", queriedSequence)
		}
		if len(queriedInbound) != 4 {
			t.Errorf("query expected 4 inbound records to be stored, got %d", len(queriedInbound))
		}
		if len(queriedOutbound) != 2 {
			t.Errorf("query expected 2 outbound records to be stored, got %d", len(queriedOutbound))
		}
		expected := &BatchState{
			BatchSize:      2,
			BatchesEmitted: 2,
		}
		if diff := cmp.Diff(expected, queriedState); diff != "" {
			t.Error("unexpected state after query")
			t.Error(diff)
		}
	})

	t.Run("the state can be verified by reprocessing the queried inbound events", func(t *testing.T) {
		recalculatedState := NewBatchState()
		var recalculatedOutbound []OutboundEvent
		for i := 0; i < len(queriedInbound); i++ {
			recalculatedOutbound = append(recalculatedOutbound, recalculatedState.Process(queriedInbound[i])...)
		}
		if diff := cmp.Diff(queriedState, recalculatedState); diff != "" {
			t.Error("unexpected state after recalculation")
			t.Error(diff)
		}
		if diff := cmp.Diff(queriedOutbound, recalculatedOutbound); diff != "" {
			t.Error("unexpected outbound events")
			t.Error(diff)
		}
	})

	t.Run("the state can be overwritten if required, e.g. by reprocessing InboundEvent records if Process function logic has changed", func(t *testing.T) {
		overwriteStateWith := NewBatchState()
		overwriteStateWith.Values = []int{6, 5, 4}
		newOutbound := []OutboundEvent{
			&BatchOutput{Numbers: []int{6, 5, 4}},
		}
		// Don't pass any inbound events.
		// This code is an example of how it's possible to make a change to the state,
		// and send an arbitrary oubound message. This might be required in the case of
		// repairing state data after fixing a production bug.
		err := s.Put("id", 1, overwriteStateWith, nil, newOutbound)
		if err != nil {
			t.Fatalf("failed to overwrite state: %v", err)
		}
		getState := NewBatchState()
		seq, err := s.Get("id", getState)
		if err != nil {
			t.Fatalf("failed to get state: %v", err)
		}
		if seq != 2 {
			t.Errorf("expected overwritten state to increment the sequence number, but got sequence: %d", seq)
		}
		if diff := cmp.Diff(overwriteStateWith, getState); diff != "" {
			t.Error("unexpected state after overwrite")
			t.Error(diff)
		}
	})
}
