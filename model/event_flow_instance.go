package model

import "time"

type EventFlowInstance struct {
	Id               int64
	FlowId           string
	Name             string
	Type             string
	Data             string
	Status           string
	CurrentEventName string
	CreateAt         *time.Time
	UpdateAt         *time.Time
}

const (
	FlowStatusPending    = "pending"
	FlowStatusProcessing = "processing"
)
