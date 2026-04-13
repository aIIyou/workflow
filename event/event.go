package event

import (
	"context"
	"time"

	"github.com/aIIyou/workflow/model"
	"github.com/aIIyou/workflow/storage/adapter"
	"github.com/google/uuid"
)

const (
	StatusCanceled   = "canceled"
	StatusFailed     = "failed"
	StatusFinished   = "finished"
	StatusPaused     = "paused"
	StatusPending    = "pending"
	StatusProcessing = "processing"
)

type Event struct {
	EventId string

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

	//FlowName is the workflow name
	FlowName string

	VisibleAt *time.Time
}

func NewFromModel(event *model.Event) *Event {
	if event == nil {
		return nil
	}

	return &Event{
		EventId:   event.EventId,
		Type:      event.Type,
		Name:      event.Name,
		Async:     event.Async,
		Status:    event.Status,
		Ctx:       nil,
		Handler:   nil,
		FlowId:    event.FlowId,
		FlowType:  event.FlowType,
		FlowName:  event.FlowName,
		VisibleAt: event.VisibleAt,
	}
}

// NewEvent create a new event
func NewEvent(eventType, name string, async bool, handler func(ctx context.Context)) *Event {

	return &Event{
		EventId: uuid.NewString(),
		Type:    eventType,
		Async:   async,
		Name:    name,
		Handler: handler,
	}
}

func StartNewEvent(ctx context.Context, e *Event) error {
	modelEvent := &model.Event{
		EventId:     uuid.NewString(),
		Type:        e.Type,
		Async:       e.Async,
		Name:        e.Name,
		Status:      "Pending",
		FlowId:      e.FlowId,
		FlowType:    e.FlowType,
		FlowName:    e.FlowName,
		CreateAt:    nil,
		UpdateAt:    nil,
		HeartBeatAt: nil,
		VisibleAt:   e.VisibleAt,
		WorkerIP:    "",
		WorkerId:    "",
	}
	err := adapter.CreateEvent(ctx, modelEvent)
	if err != nil {
		return err
	}
	return nil
}

// SetId set event id
func (e *Event) SetId(id string) *Event {
	e.EventId = id
	return e
}

// StartEvent is the start pseudo-event which is used to start the event flow and has no actual meaning
type StartEvent struct {
	Event
}

var GlobalStartEvent = &StartEvent{
	Event: Event{
		EventId: uuid.New().String(),
		Type:    "start",
		Name:    "global-start",
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
		EventId: uuid.New().String(),
		Type:    "end",
		Name:    "global-end",
		Handler: func(ctx context.Context) {
		},
	},
}

func LoadEvent(eventId string) (*Event, error) {
	return nil, nil
}
