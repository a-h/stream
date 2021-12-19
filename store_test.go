package stream

import (
	"testing"

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
