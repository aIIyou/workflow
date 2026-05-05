package flow

import (
	"context"
	"fmt"
	"time"

	"github.com/aIIyou/workflow/model"
	"github.com/aIIyou/workflow/storage/adapter"
)

type IEventFlowInstance interface {
}

type EventFlowInstance struct {
	Id               int64
	FlowId           string
	Name             string
	Data             string
	Status           string
	CurrentEventName string
	CreateAt         *time.Time
	UpdateAt         *time.Time
}

func (flow *EventFlowInstance) NewFromModel(instance *model.EventFlowInstance) *EventFlowInstance {
	return &EventFlowInstance{
		Id:               instance.Id,
		FlowId:           instance.FlowId,
		Name:             instance.Name,
		Data:             instance.Data,
		Status:           instance.Status,
		CurrentEventName: instance.CurrentEventName,
		CreateAt:         instance.CreateAt,
		UpdateAt:         instance.UpdateAt,
	}
}

func (flow *EventFlowInstance) RetrieveEventFlowData(ctx context.Context) (string, error) {
	if flow == nil {
		return "", fmt.Errorf(`event flow instance is nil`)
	}
	return flow.Data, nil
}

func (flow *EventFlowInstance) RetrieveEventFlowInstance(ctx context.Context, flowId string) (*EventFlowInstance, error) {
	eventFlowInstance, err := adapter.RetrieveEventFlowInstance(ctx, flowId)
	if err != nil {
		return nil, err
	}
	return flow.NewFromModel(eventFlowInstance), nil
}

func (flow *EventFlowInstance) UpdateStatus(ctx context.Context, status string) error {
	a, err := adapter.RetrieveAdapter(adapter.GF)
	if err != nil {
		return err
	}
	return a.UpdateFlowStatus(ctx, flow.FlowId, status)
}
