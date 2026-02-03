package event

import (
	"context"

	"github.com/google/uuid"
)

const (
	StatusCanceled = "canceled"
	StatusFailed   = "failed"
	StatusFinished = "finished"
	StatusPaused   = "paused"
	StatusPending  = "pending"
)

type Event struct {
	Id string

	// Type is the event category determined by the user's business logic layer.
	// Examples: "payment.processed", "user.registered", "order.shipped"
	Type string

	// Name is the user-defined identifier for this specific event instance.
	// This provides human-readable context for monitoring and debugging.
	Name string

	//Async is used to distinguish whether an event is processed synchronously or asynchronously
	Async bool

	// Status is the event status
	// when event is created,the status is "pending"
	Status string

	Ctx context.Context

	// Handler is the event handler
	Handler func(ctx context.Context)

	//FlowId is the workflow id
	FlowId string

	//FlowType is the workflow type
	FlowType string
}

// NewEvent create a new event
func NewEvent(eventType, name string, async bool, handler func(ctx context.Context)) *Event {

	return &Event{
		Id:      uuid.NewString(),
		Type:    eventType,
		Async:   async,
		Name:    name,
		Handler: handler,
	}
}

// SetId set event id
func (e *Event) SetId(id string) *Event {
	e.Id = id
	return e
}

// StartEvent is the start pseudo-event which is used to start the event flow and has no actual meaning
type StartEvent struct {
	Event
}

var GlobalStartEvent = &StartEvent{
	Event: Event{
		Id:   uuid.New().String(),
		Type: "start",
		Name: "global-start",
		Handler: func(ctx context.Context) {
		},
	},
}

// EndEvent is the end pseudo-event which is used to end the event flow and has no actual meaning
type EndEvent struct {
	Event
}

var GlobalEndEvent = &EndEvent{
	Event: Event{
		Id:   uuid.New().String(),
		Type: "end",
		Name: "global-end",
		Handler: func(ctx context.Context) {
		},
	},
}

func LoadEvent(eventId string) (*Event, error) {
	return nil, nil
}
