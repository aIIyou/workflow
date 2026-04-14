package model

import "time"

type Event struct {
	Id          int64
	EventId     string
	Type        string
	Async       bool
	Name        string
	Status      string
	FlowId      string
	FlowType    string
	FlowName    string
	CreateAt    *time.Time
	UpdateAt    *time.Time
	HeartBeatAt *time.Time
	VisibleAt   *time.Time
	WorkerIP    string
	WorkerId    string
}

const (
	EventStatusPending    = "pending"
	EventStatusProcessing = "processing"
)
