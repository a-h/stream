package stream

import (
	"errors"
	"reflect"
)

// State of the entity.
type State interface {
	Process(event InboundEvent) (outbound []OutboundEvent)
}

// InboundEvents are received from external systems.
type InboundEvent interface {
	EventName() string
	IsInbound()
}

// OutboundEvents are send via EventBridge.
type OutboundEvent interface {
	EventName() string
	IsOutbound()
}

// Store is the interface that describes database operations.
type Store interface {
	Get(id string, state State) (sequence int64, err error)
	Put(id string, atSequence int64, state State, inbound []InboundEvent, output []OutboundEvent) error
}

// Processor of events.
type Processor struct {
	store    Store
	id       string
	state    State
	sequence int64
}

// New creates a new, empty stream processor.
func New(store Store, id string, state State) (p *Processor, err error) {
	if reflect.ValueOf(state).Kind() != reflect.Ptr {
		err = errors.New("the state parameter must be a pointer")
		return
	}
	p = &Processor{
		store:    store,
		id:       id,
		state:    state,
		sequence: 0,
	}
	return
}

// Load the state from the data store. Pass a pointer to the state.
func Load(store Store, id string, state State) (p *Processor, err error) {
	if reflect.ValueOf(state).Kind() != reflect.Ptr {
		err = errors.New("the state parameter must be a pointer")
		return
	}
	sequence, err := store.Get(id, state)
	if err != nil {
		return
	}
	p = &Processor{
		store:    store,
		id:       id,
		state:    state,
		sequence: sequence,
	}
	return
}

// Process inbound events, then store the updated state and outbound events.
func (p *Processor) Process(events ...InboundEvent) error {
	var inbound []InboundEvent
	var outbound []OutboundEvent
	for i := 0; i < len(events); i++ {
		inbound = append(inbound, events[i])
		outbound = append(outbound, p.state.Process(events[i])...)
	}
	return p.store.Put(p.id, p.sequence, p.state, inbound, outbound)
}
