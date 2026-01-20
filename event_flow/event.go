package event_flow

import "context"

type Event struct {
	Id int64

	// Type is the event category determined by the user's business logic layer.
	// Examples: "payment.processed", "user.registered", "order.shipped"
	Type string

	// Name is the user-defined identifier for this specific event instance.
	// This provides human-readable context for monitoring and debugging.
	Name string

	ctx context.Context

	// Handler is the event handler
	Handler func(ctx context.Context)

	//FlowId is the workflow id
	FlowId int64

	//FlowType is the workflow type
	FlowType string
}

// NewEvent create a new event
func NewEvent(eventType, name string, handler func(ctx context.Context)) *Event {
	return &Event{
		Type:    eventType,
		Name:    name,
		Handler: handler,
	}
}

// SetId set event id
func (e *Event) SetId(id int64) *Event {
	e.Id = id
	return e
}

// StartEvent is the start pseudo-event which is used to start the event flow and has no actual meaning
type StartEvent struct {
	Event
}

var GlobalStartEvent = &StartEvent{
	Event: Event{
		Id:   0,
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
		Id:   0,
		Type: "end",
		Name: "global-end",
		Handler: func(ctx context.Context) {
		},
	},
}
