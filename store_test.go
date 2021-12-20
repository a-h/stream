package stream

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
)

func TestGetStateNotFoundIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(name, "Average", WithRegion(region), WithClient(testClient))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	as := &AverageState{}

	// Act.
	_, err = s.Get("id", as)

	// Assert.
	if err == nil {
		t.Error("expected ErrStateNotFound, got nil")
	}
	if diff := cmp.Diff(ErrStateNotFound.Error(), err.Error()); diff != "" {
		t.Error(diff)
	}
}

func TestPutStateIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(name, "Average", WithRegion(region), WithClient(testClient))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	as := &AverageState{}

	// Act.
	err = s.Put("id", 0, as, nil, nil)

	// Assert.
	if err != nil {
		t.Errorf("unexpected error writing initial state: %v", err)
	}
}

func TestPutStateWithHistoryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(name, "Average", WithRegion(region), WithClient(testClient), WithPersistStateHistory(true))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	as := &AverageState{}

	// Act.
	err = s.Put("id", 0, as, nil, nil)

	if err != nil {
		t.Errorf("unexpected error writing initial state: %v", err)
	}

	err = s.Put("id", 1, as, nil, nil)

	if err != nil {
		t.Errorf("unexpected error writing updated state: %v", err)
	}

}

func TestPutStateCannotOverwriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(name, "Average", WithRegion(region), WithClient(testClient))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	as := &AverageState{}
	err = s.Put("id", 0, as, nil, nil)
	if err != nil {
		t.Errorf("unexpected error writing initial state: %v", err)
	}

	// Act.
	err = s.Put("id", 0, as, nil, nil)
	if err != ErrOptimisticConcurrency {
		t.Errorf("expected error overwriting an existing version number, but got: %v", err)
	}
}

// AliasedAverageState is used to test the JSON encoding. We need to use a
// different (or aliased) type to AverageState as attributevalue.Encoder.Encode
// uses a cache internally when looking up the attribute name to use, in order
// to prevent repeated calls the reflect.TypeOf. This has the unfortunate
// side-effect of causing this test fail as the other tests run first and the
// cache gets populated with the raw field names rather than the one specified
// in the json struct tag. It has the other unfortunate side-effect of deleting
// HOURS from life trying to understand why this test would fail with
// AverageState but not another identical type!
type AliasedAverageState struct {
	AverageState
}

func (s *AliasedAverageState) Process(event InboundEvent) (outbound []OutboundEvent, err error) {
	return s.AverageState.Process(event)
}

func TestJSONCodecIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(name, "Average", WithRegion(region), WithClient(testClient), WithCodecTag("json"))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	as := AliasedAverageState{AverageState: AverageState{Sum: 1}}

	if err != nil {
		t.Errorf("failed to encode: %v", err)
	}

	// Act.
	err = s.Put("id", 0, &as, nil, nil)

	// Assert.
	if err != nil {
		t.Errorf("unexpected error writing initial state: %v", err)
	}

	// Ensure state was marshalled using JSON struct tags
	out, err := s.Client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: &name,
		Key: map[string]types.AttributeValue{
			"_pk": &types.AttributeValueMemberS{Value: "Average/id"},
			"_sk": &types.AttributeValueMemberS{Value: "STATE"},
		},
	})
	if err != nil {
		t.Errorf("unexpected error getting item: %v", err)
	}
	if _, ok := out.Item["sum"]; !ok {
		t.Errorf("unexpected attribute name %q, but it wasn't found: %v", "sum", out.Item)
	}
}

func TestGetStateIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Arrange.
	name := createLocalTable(t)
	defer deleteLocalTable(t, name)
	s, err := NewStore(name, "Average", WithRegion(region), WithClient(testClient))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	initial := &AverageState{
		Sum:   1,
		Count: 1,
		Value: 1,
	}
	err = s.Put("id", 0, initial, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error writing initial state: %v", err)
	}

	// Act.
	retrieved := &AverageState{}
	sequence, err := s.Get("id", retrieved)
	if err != nil {
		t.Errorf("unexpected error getting state from DB: %v", err)
	}

	// Assert.
	if sequence != 1 {
		t.Errorf("expected incremented sequence, got %d", sequence)
	}
	if initial.Sum != retrieved.Sum {
		t.Errorf("expected sums to match, but got %d and %d", initial.Sum, retrieved.Sum)
	}
	if diff := cmp.Diff(initial, retrieved); diff != "" {
		t.Error(diff)
	}
}
