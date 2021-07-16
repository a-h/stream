# stream

DynamoDB backed event sourced database library.

## Usage

Define your state model by implementing the `state.State` interface. It has a single method that processes inbound events, and returns outbound events to send.

Here's an example that processes inbound events and when the batch size of input events is reached and outbound event is sent.

```go
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

func (s *BatchState) Process(event InboundEvent) (outbound []OutboundEvent, err error) {
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
```

The inbound and outbound events must implement the `InboundEvent` and `OutboundEvent` interfaces respecively:

```go
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
```

## Examples

See the `./example` directory for a complete example.
