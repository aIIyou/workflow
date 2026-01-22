package adapter

import (
	"context"
	"fmt"

	"github.com/aIIyou/workflow/event"
	"github.com/aIIyou/workflow/storage/mysql"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

func NewGfAdapter(name string) *GfAdapter {
	return &GfAdapter{
		group: name,
	}
}

type GfAdapter struct {

	//group the name of goframe database group
	group string
}

func (gfa *GfAdapter) CreateEvent(ctx context.Context, e *event.Event) error {

	// whether to commit or rollback transaction locally
	localTx := false

	// retrieve transaction manager from ctx
	// assuming user start a database transaction already
	// if not, start it locally
	tx := gdb.TXFromCtx(ctx, gfa.group)

	// user has not started a transaction
	var err error
	if tx == nil {
		localTx = true
		tx, err = g.DB(gfa.group).Begin(ctx)
		if err != nil {
			return err
		}
	}

	//insert event into table `event_queue`
	//note: the status of event is pending
	//note: the event is not processed util worker query it
	var (
		eventId     = e.Id
		eventType   = e.Type
		eventName   = e.Name
		eventStatus = event.StatusPending
		flowId      = e.FlowId
		flowType    = e.FlowType
	)
	_, err = tx.Exec(mysql.CreateEvent, eventId, eventType, eventName, eventStatus, flowId, flowType)
	if err != nil {

		// if start transaction locally,must commit or rollback
		if localTx {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("exec failed: %w; rollback also failed: %v", err, rollbackErr)
			}
			return fmt.Errorf(`exec failed: %v`, err)
		}
	}

	// if start transaction locally,must commit or rollback
	if localTx {
		if commitErr := tx.Commit(); commitErr != nil {
			return fmt.Errorf(`commit failed: %v`, commitErr)
		}
	}
	return nil
}

func (gfa *GfAdapter) RetrievePendingEvent(ctx context.Context) (*event.Event, error) {
	result, err := g.DB(gfa.group).Query(ctx, mysql.RetrievePendingEvent)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}
	events := make([]*event.Event, 0)
	err = result.Structs(&events)
	if err != nil {
		return nil, err
	}
	return events[0], nil
}
