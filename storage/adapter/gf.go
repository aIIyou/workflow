package adapter

import (
	"context"
	"fmt"

	flow "github.com/aIIyou/workflow/event_flow"
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

func (gfa *GfAdapter) CreateEvent(ctx context.Context, event *flow.Event) error {

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
		eventId     = event.Id
		eventType   = event.Type
		eventName   = event.Name
		eventStatus = flow.StatusPending
		flowId      = event.FlowId
		flowType    = event.FlowType
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
