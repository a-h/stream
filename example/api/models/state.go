package models

import (
	"errors"
	"math/rand"

	"github.com/a-h/stream"
)

func NewSlotMachine(id string) *SlotMachine {
	return &SlotMachine{
		ID:        id,
		Balance:   0,
		Payout:    4,
		WinChance: 0.18,
	}
}

func Win(chance float64) bool { return rand.Float64() <= chance }

var ErrCannotInsertCoin = errors.New("cannot insert coin")
var ErrCannotPullHandle = errors.New("cannot pull handle")

type SlotMachine struct {
	ID      string `json:"id"`
	Balance int    `json:"balance"`
	// How much is paid out if you win.
	Payout int `json:"payout"`
	// How likely you are to get paid out.
	WinChance    float64 `json:"winChance"`
	Games        int     `json:"games"`
	Wins         int     `json:"wins"`
	Losses       int     `json:"losses"`
	IsCoinInSlot bool    `json:"isCoinInSlot"`
}

func (s *SlotMachine) Process(event stream.InboundEvent) (outbound []stream.OutboundEvent, err error) {
	switch e := event.(type) {
	case InsertCoin:
		return s.InsertCoin()
	case PullHandle:
		return s.PullHandle(e)
	}
	return
}

func (s *SlotMachine) InsertCoin() (outbound []stream.OutboundEvent, err error) {
	if s.IsCoinInSlot {
		return nil, ErrCannotInsertCoin
	}
	s.IsCoinInSlot = true
	return
}

func (s *SlotMachine) PullHandle(e PullHandle) (outbound []stream.OutboundEvent, err error) {
	// Complain if we can't take the coin.
	if !s.IsCoinInSlot {
		return nil, ErrCannotPullHandle
	}
	// Take the coin.
	s.IsCoinInSlot = false

	// Update the stats.
	won := Win(s.WinChance)
	s.Games++
	if won {
		s.Wins++
		s.Balance -= (s.Payout - 1)
	} else {
		s.Losses++
		s.Balance++
	}

	// Send events.
	outbound = append(outbound, GamePlayed{
		MachineID: s.ID,
		Won:       won,
	})
	if won {
		outbound = append(outbound, PayoutMade{
			UserID: e.UserID,
			Amount: s.Payout,
		})
	}
	return
}

// Input events.
type InsertCoin struct {
}

func (_ InsertCoin) EventName() string { return "InsertCoin" }
func (_ InsertCoin) IsInbound()        {}

type PullHandle struct {
	UserID string `json:"userId"`
}

func (_ PullHandle) EventName() string { return "PullHandle" }
func (_ PullHandle) IsInbound()        {}

// Output events.
type GamePlayed struct {
	MachineID string `json:"machineId"`
	Won       bool   `json:"won"`
}

func (_ GamePlayed) EventName() string { return "GamePlayed" }
func (_ GamePlayed) IsOutbound()       {}

type PayoutMade struct {
	UserID string `json:"userId"`
	Amount int    `json:"amount"`
}

func (_ PayoutMade) EventName() string { return "PayoutMade" }
func (_ PayoutMade) IsOutbound()       {}
