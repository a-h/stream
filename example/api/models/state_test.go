package models

import (
	"testing"

	"github.com/a-h/stream"
	"github.com/google/go-cmp/cmp"
)

func TestState(t *testing.T) {
	id := "id"
	tests := []struct {
		name           string
		start          *SlotMachine
		previous       []stream.InboundEvent
		current        stream.InboundEvent
		expected       *SlotMachine
		expectedEvents []stream.OutboundEvent
		expectedErr    error
	}{
		{
			name:     "it is possible to insert a coin into a machine",
			previous: []stream.InboundEvent{},
			current:  InsertCoin{},
			expected: func() *SlotMachine {
				m := NewSlotMachine(id)
				m.IsCoinInSlot = true
				return m
			}(),
		},
		{
			name: "it is not possible to insert more than one coin into a machine",
			previous: []stream.InboundEvent{
				InsertCoin{},
			},
			current: InsertCoin{},
			expected: func() *SlotMachine {
				m := NewSlotMachine("id")
				m.IsCoinInSlot = true
				return m
			}(),
			expectedErr: ErrCannotInsertCoin,
		},
		{
			name:        "it is not possible to pull the handle if a coin hasn't been inserted",
			previous:    []stream.InboundEvent{},
			current:     PullHandle{},
			expected:    NewSlotMachine(id),
			expectedErr: ErrCannotPullHandle,
		},
		{
			name: "if a coin has been inserted, it's possible to play the game and win",
			start: func() *SlotMachine {
				m := NewSlotMachine(id)
				m.WinChance = 1.0
				return m
			}(),
			previous: []stream.InboundEvent{
				InsertCoin{},
			},
			current: PullHandle{},
			expected: func() *SlotMachine {
				m := NewSlotMachine(id)
				m.WinChance = 1.0
				m.Balance = -3
				m.Games = 1
				m.Wins = 1
				return m
			}(),
			expectedEvents: []stream.OutboundEvent{
				GameResult{MachineID: "id", Win: true},
				Payout{Amount: 4},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		actual := tt.start
		if actual == nil {
			actual = NewSlotMachine(id)
		}
		for i := 0; i < len(tt.previous); i++ {
			_, err := actual.Process(tt.previous[i])
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
		actualEvents, err := actual.Process(tt.current)
		if tt.expectedErr != err {
			t.Errorf("expected error '%v', got '%v'", tt.expectedErr, err)
		}
		if diff := cmp.Diff(tt.expected, actual); diff != "" {
			t.Errorf("unexpected state:\n%v", diff)
		}
		if diff := cmp.Diff(tt.expectedEvents, actualEvents); diff != "" {
			t.Errorf("unexpected events:\n%v", diff)
		}
	}
}
