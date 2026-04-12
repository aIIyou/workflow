package flow

import (
	"context"
	"testing"
	"time"

	"github.com/aIIyou/workflow/config"
	"github.com/aIIyou/workflow/event"
	"github.com/aIIyou/workflow/model"
	"github.com/aIIyou/workflow/storage/adapter"
	"github.com/google/uuid"
)

type UnitTestHandler struct{}

func (h UnitTestHandler) StartEvent(ctx context.Context) error {
	return nil
}
func (h UnitTestHandler) EndEvent(ctx context.Context) error {
	return nil
}

type UnitTestAdapter struct {
}

func (u UnitTestAdapter) CreateEvent(ctx context.Context, event *model.Event) error {
	//TODO implement me
	panic("implement me")
}

func (u UnitTestAdapter) RetrievePendingEvent(ctx context.Context) (*model.Event, error) {
	//TODO implement me
	panic("implement me")
}

func (u UnitTestAdapter) RetrieveExpiredEvent(ctx context.Context) (*model.Event, error) {
	//TODO implement me
	panic("implement me")
}

func (u UnitTestAdapter) RetrieveFlowPendingEvent(ctx context.Context, flowId string) (*model.Event, error) {
	//TODO implement me
	panic("implement me")
}

func (u UnitTestAdapter) RetrieveEventFlowInstance(ctx context.Context, flowId string) (*model.EventFlowInstance, error) {
	return &model.EventFlowInstance{
		Id:               1,
		FlowId:           uuid.NewString(),
		Name:             "unit_test_flow",
		Data:             `{"start_event_success":true}`,
		Status:           "running",
		CurrentEventName: "start_event",
		CreateAt:         &time.Time{},
		UpdateAt:         &time.Time{},
	}, nil
}

func (u UnitTestAdapter) UpdateEventHeartbeat(ctx context.Context, eventId string) error {
	//TODO implement me
	panic("implement me")
}

func (u UnitTestAdapter) UpdateEventFlowData(ctx context.Context, flowId string, data string) error {
	return nil
}

func TestFlow_NextEvent(t *testing.T) {

	//register unit test adapter
	if err := adapter.RegisterAdapter("unit_test", UnitTestAdapter{}); err != nil {
		t.Errorf(`RegisterAdapter failed %v`, err)
	}
	adapter.SetFrameworkName(adapter.UT)
	//test flow
	conf := &config.Configuration{
		MaxWorker: 0,
		Flow: []config.FlowConfig{
			{
				FlowName: "unit_test_flow",
				Event: []config.EventConfig{
					{
						Name:  "start_event",
						Async: true,
					},
					{
						Name:  "end_event",
						Async: true,
					},
				},
				StartEvent: "start_event",
				Transitions: []config.Transition{
					{
						FromEvent: "start_event",
						ToEvent:   "end_event",
						Expr:      "start_event_success == true",
					},
				},
			},
		},
	}
	handler := UnitTestHandler{}
	if err := RegisterEventflow("unit_test_flow", handler, conf); err != nil {
		t.Errorf(`RegisterEventFlow error %v`, err)
	}

	flow, err := RetrieveEventflow("unit_test_flow")
	if err != nil {
		t.Errorf(`RetrieveEventFlow failed %v`, err)
	}

	e := &event.Event{
		EventId:  uuid.NewString(),
		Type:     "start_event",
		Name:     "start_event",
		Async:    true,
		Status:   "finished",
		Ctx:      context.Background(),
		Handler:  nil,
		FlowId:   uuid.NewString(),
		FlowType: "unit_test_flow",
		FlowName: "unit_test_flow",
	}
	data := make(map[string]any)
	innerMap := make(map[string]any)
	innerMap["start_event_success"] = true
	data[KeyControlData] = innerMap

	if err := SetContextData(context.Background(), "test", data); err != nil {
		t.Errorf(`SetContextData failed %v`, err)
	}

	if eventName, err := flow.NextEvent(e); err != nil {
		t.Errorf(`flow.NextEvent failed %v`, err)
	} else if eventName != "end_event" {
		t.Errorf(`eventName want "end_event",but get %s`, eventName)
	} else {
		t.Log(`pass`)
	}

}
