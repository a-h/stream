package models

import (
	"errors"
	"math/rand"

	"github.com/a-h/stream"
)

func NewSlotMachine() *SlotMachine {
	return &SlotMachine{}
}

var ErrCannotInsertCoin = errors.New("cannot insert coin")
var ErrCannotPullHandle = errors.New("cannot pull handle")

type SlotMachine struct {
	ID      string
	Balance int
	// How much is paid out if you win.
	Payout int
	// How likely you are to get paid out.
	WinChance    float64
	Games        int
	Wins         int
	Losses       int
	IsCoinInSlot bool
}

func (s *SlotMachine) Process(event stream.InboundEvent) (outbound []stream.OutboundEvent, err error) {
	switch e := event.(type) {
	case InsertCoin:
		ok := s.InsertCoin()
		if !ok {
			err = ErrCannotInsertCoin
			return
		}
		break
	case PullHandle:
		win, ok := s.PullHandle()
		if !ok {
			err = ErrCannotPullHandle
		}
		outbound = append(outbound, GameResult{
			MachineID: s.ID,
			Win:       win,
		})
		if win {
			outbound = append(outbound, Payout{
				UserID: e.UserID,
				Amount: s.Payout,
			})
		}
	}
	return
}

func (s *SlotMachine) InsertCoin() (ok bool) {
	if s.IsCoinInSlot {
		return false
	}
	s.IsCoinInSlot = true
	return true
}

func (s *SlotMachine) PullHandle() (win bool, ok bool) {
	// Complain if we can't take the coin.
	ok = s.IsCoinInSlot
	if !ok {
		return
	}
	s.IsCoinInSlot = false

	// See if we win.
	win = rand.Float64() <= s.WinChance

	// Update the stats.
	s.Games++
	if win {
		s.Wins++
		s.Balance -= (s.Payout - 1)
	} else {
		s.Losses--
		s.Balance++
	}
	return
}

// Input events.
type InsertCoin struct {
}

func (_ InsertCoin) EventName() string { return "InsertCoin" }
func (_ InsertCoin) IsInbound()        {}

type PullHandle struct {
	UserID string
}

func (_ PullHandle) EventName() string { return "PullHandle" }
func (_ PullHandle) IsInbound()        {}

// Output events.
type GameResult struct {
	MachineID string
	Win       bool
}

func (_ GameResult) EventName() string { return "GameResult" }
func (_ GameResult) IsOutbound()       {}

type Payout struct {
	UserID string
	Amount int
}

func (_ Payout) EventName() string { return "Payout" }
func (_ Payout) IsOutbound()       {}
