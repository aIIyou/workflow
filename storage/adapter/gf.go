package adapter

import (
	"context"
	"fmt"
	"time"

	"github.com/aIIyou/workflow/model"
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

func (gfa *GfAdapter) CreateEvent(ctx context.Context, e *model.Event) error {

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
		eventId     = e.EventId
		eventType   = e.Type
		eventName   = e.Name
		eventStatus = "Pending"
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

// RetrievePendingEvent query an async event whose status is `pending` and update it status to choose
// using mysql transaction
func (gfa *GfAdapter) RetrievePendingEvent(ctx context.Context) (*model.Event, error) {
	tx, err := g.DB(gfa.group).Begin(ctx)
	if err != nil {
		return nil, err
	}
	result, err := tx.Query(mysql.RetrievePendingEvent)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}
	events := make([]*model.Event, 0)
	err = result.Structs(&events)
	if err != nil {
		return nil, err
	}

	pendingEvent := events[0]
	if _, err = tx.Exec(mysql.UpdateEventStatus, "processing", pendingEvent.EventId); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	pendingEvent.Status = "choose"

	return pendingEvent, nil
}

func (gfa *GfAdapter) RetrieveFlowPendingEvent(ctx context.Context, flowId string) (*model.Event, error) {
	var (
		localTx bool
	)
	tx := gdb.TXFromCtx(ctx, gfa.group)
	if tx == nil {
		tx = gdb.TXFromCtx(ctx, gfa.group)
		localTx = true
	}
	result, err := tx.Query(mysql.RetrieveFlowPendingEvent, flowId)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}
	if result.Len() > 1 {
		panic(fmt.Sprintf(`here are more than 1 pending event of flow "%s"`, flowId))
	}
	events := make([]*model.Event, 0)
	err = result.Structs(&events)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(mysql.UpdatePendingEventStatus, flowId)
	if err != nil {
		if localTx {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return nil, fmt.Errorf("exec failed: %w; rollback also failed: %v", err, rollbackErr)
			}
		}
		return nil, fmt.Errorf(`exec failed: %v`, err)
	}
	if localTx {
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, fmt.Errorf("commit failed: %w", commitErr)
		}
	}
	events[0].Status = "Processing"
	return events[0], nil
}

func (gfa *GfAdapter) RetrieveEventFlowInstance(ctx context.Context, flowId string) (*model.EventFlowInstance, error) {
	result, err := g.DB(gfa.group).Query(ctx, mysql.RetrieveEventFlow, flowId)
	if err != nil {
		return nil, err
	}
	if result.Len() <= 0 {
		return nil, nil
	}
	eventFlows := make([]*model.EventFlowInstance, 0)
	err = result.Structs(&eventFlows)
	if err != nil {
		return nil, err
	}
	return eventFlows[0], nil
}

func (gfa *GfAdapter) UpdateEventHeartbeat(ctx context.Context, eventId string) error {
	_, err := g.DB(gfa.group).Exec(ctx, mysql.UpdateEventHeartbeat, eventId)
	if err != nil {
		return fmt.Errorf("failed to update event heartbeat: %v", err)
	}
	return nil
}

func (gfa *GfAdapter) RetrieveFlowCurrentEvent(ctx context.Context, flowId string) (*model.Event, error) {
	result, err := g.DB(gfa.group).Query(ctx, mysql.RetrieveFlowCurrentEvent, flowId)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}

	events := make([]*model.Event, 0)
	err = result.Structs(&events)
	if err != nil {
		return nil, err
	}

	return events[0], nil
}

func (gfa *GfAdapter) UpdateEventVisibleAt(ctx context.Context, eventId string, visibleAt time.Time) error {
	_, err := g.DB(gfa.group).Exec(ctx, mysql.UpdateEventVisibleAt, visibleAt, eventId)
	if err != nil {
		return fmt.Errorf("failed to update event visible_at: %v", err)
	}
	return nil
}

func (gfa *GfAdapter) UpdateEventFlowData(ctx context.Context, flowId string, data string) error {
	_, err := g.DB(gfa.group).Exec(ctx, mysql.UpdateEventFlowData, data, flowId)
	if err != nil {
		return fmt.Errorf("failed to update event flow data: %v", err)
	}
	return nil
}
