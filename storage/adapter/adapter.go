package adapter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aIIyou/workflow/model"
)

type FrameworkName string

const (
	GF FrameworkName = "gf"
	GO FrameworkName = "gorm"
	UT FrameworkName = "unit_test"
)

var (
	framework          = GF
	frameworkOnceMutex sync.Once
)

func SetFrameworkName(name FrameworkName) {
	f := func() {
		framework = name
	}
	frameworkOnceMutex.Do(f)
}

var (
	adapters      map[FrameworkName]Adapter
	adaptersMutex sync.RWMutex
)

type Adapter interface {
	StartEventFlow(ctx context.Context, instance *model.EventFlowInstance, event *model.Event) error

	//CreateEvent insert event into table `event_queue`
	//Canonical adapter must use transaction to make insert and user logic atomic.
	CreateEvent(ctx context.Context, event *model.Event) error

	// RetrievePendingEvent retrieves the next pending event from the event queue.
	// This method should return the oldest pending event that is ready for processing.
	// Returns:
	//   - *event.Event: The retrieved pending event, or nil if no pending events are available
	//   - error: Any error encountered during retrieval, such as database connection issues
	RetrievePendingEvent(ctx context.Context) (*model.Event, error)

	RetrieveExpiredEvent(ctx context.Context) (*model.Event, error)

	//RetrieveFlowPendingEvent get the pending events from the specified flow
	//!!!
	//the event system is designed to restrict a flow to only allow
	//one event in the pending state to exist at the same time.
	//!!!
	RetrieveFlowPendingEvent(ctx context.Context, flowId string) (*model.Event, error)

	//RetrieveFlowCurrentEvent retrieves the current event from the specified flow
	//This method returns the current event in the flow regardless of its status
	RetrieveFlowCurrentEvent(ctx context.Context, flowId string) (*model.Event, error)

	//RetrieveEventFlowInstance retrieves the event flow which flow_id equals @flowId
	RetrieveEventFlowInstance(ctx context.Context, flowId string) (*model.EventFlowInstance, error)

	//UpdateEventHeartbeat updates the heartbeat timestamp for the specified event
	UpdateEventHeartbeat(ctx context.Context, eventId string) error

	//UpdateEventVisibleAt updates the visible_at timestamp for the specified event
	UpdateEventVisibleAt(ctx context.Context, eventId string, visibleAt time.Time) error

	//UpdateEventFlowData updates the data field of the specified event flow instance
	UpdateEventFlowData(ctx context.Context, flowId string, data string) error

	//UpdateEventStatus updates the status of the specified event
	UpdateEventStatus(ctx context.Context, eventId string, status string) error

	UpdateFlowStatus(ctx context.Context, status string, flowId string) error
}

func RegisterAdapter(name FrameworkName, adapter Adapter) error {
	adaptersMutex.Lock()
	defer adaptersMutex.Unlock()
	if adapters == nil {
		adapters = make(map[FrameworkName]Adapter)
	}
	if _, existed := adapters[name]; existed {
		panic(fmt.Sprintf(`%s adapter has already registered`, name))
	}
	adapters[name] = adapter
	return nil
}

func RetrieveAdapter(name FrameworkName) (Adapter, error) {
	adaptersMutex.RLock()
	defer adaptersMutex.RUnlock()
	if adapter, existed := adapters[name]; !existed {
		return nil, fmt.Errorf(`%s adapter has not been registered`, name)
	} else {
		return adapter, nil
	}
}

// default ORM framework unified operation entrance
// the default orm framework is specified by the global variable framework
// user can use function SetFrameworkName to specify global default ORM framework
// note: global default ORM framework can only be specified once

func StartEventFlow(ctx context.Context, instance *model.EventFlowInstance, event *model.Event) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.StartEventFlow(ctx, instance, event)
}
func CreateEvent(ctx context.Context, event *model.Event) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.CreateEvent(ctx, event)
}

func RetrievePendingEvent(ctx context.Context) (*model.Event, error) {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return nil, err
	}
	return adapter.RetrievePendingEvent(ctx)
}

func RetrieveExpiredEvent(ctx context.Context) (*model.Event, error) {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return nil, err
	}
	return adapter.RetrieveExpiredEvent(ctx)
}

func RetrieveFlowPendingEvent(ctx context.Context, flowId string) (*model.Event, error) {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return nil, err
	}
	return adapter.RetrieveFlowPendingEvent(ctx, flowId)
}

func RetrieveEventFlowInstance(ctx context.Context, flowId string) (*model.EventFlowInstance, error) {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return nil, err
	}
	return adapter.RetrieveEventFlowInstance(ctx, flowId)
}

func RetrieveFlowCurrentEvent(ctx context.Context, flowId string) (*model.Event, error) {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return nil, err
	}
	return adapter.RetrieveFlowCurrentEvent(ctx, flowId)
}

func UpdateEventHeartbeat(ctx context.Context, eventId string) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.UpdateEventHeartbeat(ctx, eventId)
}

func UpdateEventVisibleAt(ctx context.Context, eventId string, visibleAt time.Time) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.UpdateEventVisibleAt(ctx, eventId, visibleAt)
}

func UpdateEventFlowData(ctx context.Context, flowId string, data string) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.UpdateEventFlowData(ctx, flowId, data)
}

func UpdateEventStatus(ctx context.Context, eventId string, status string) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.UpdateEventStatus(ctx, eventId, status)
}
