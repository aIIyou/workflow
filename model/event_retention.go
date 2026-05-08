package model

import "time"

type EventRetention struct {
	Id        int64      `json:"id"`
	EventId   string     `json:"event_id"`
	EventName string     `json:"event_name"`
	Milestone string     `json:"milestone"`
	CreateAt  *time.Time `json:"create_at"`
	UpdateAt  *time.Time `json:"update_at"`
}
