package schedule

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aIIyou/workflow/event"
	"github.com/aIIyou/workflow/storage/adapter"
)

var (
	// scheduler global event process scheduler
	// it is guaranteed that here is only one scheduler in os specified process
	scheduler Scheduler

	//schedulerOnce scheduler could be initialized only once
	schedulerOnce sync.Once

	maintain atomic.Bool
)

const (
	eventSchedulerName = "event_scheduler"
)

// ScheduleInit
// avoid to use golang init function as initialization sequence out of control
func ScheduleInit(maxProcessor int) {
	f := func() {
		scheduler = &eventScheduler{
			name:         eventSchedulerName,
			maxProcessor: maxProcessor,
			processors:   make(map[string]Processor, maxProcessor),
		}
	}
	schedulerOnce.Do(f)
	maintain.Store(false)
	return
}

func LaunchScheduler(ctx context.Context) error {
	return scheduler.Schedule(ctx)
}

func CurrentProcessNumber(ctx context.Context) int {
	return scheduler.CurrentProcessorNumber(ctx)
}

func StartMaintain(ctx context.Context) error {
	maintain.Store(true)
	return nil
}

func CloseMaintain(ctx context.Context) error {
	maintain.Store(false)
	return nil
}

type Scheduler interface {

	//Schedule launch entry of Scheduler
	Schedule(ctx context.Context) error

	// RetrievePendingEvent retrieves the next pending event from the event queue.
	// This method should return the oldest pending event that is ready for processing.
	// Returns:
	//   - *event.Event: The retrieved pending event, or nil if no pending events are available
	//   - error: Any error encountered during retrieval, such as database connection issues
	RetrievePendingEvent(ctx context.Context) (*event.Event, error)

	// RetrieveExpiredEvent processes a pending event according to the workflow logic.
	// This method should handle the business logic for the event and update its status accordingly.
	// Parameters:
	//   - e: The event to be processed
	// Returns:
	//   - error: Any error encountered during processing, such as validation failures or execution errors
	RetrieveExpiredEvent(ctx context.Context) (*event.Event, error)

	//RetrieveFlowPendingEvent get the pending events from the specified flow
	//!!!
	//the event system is designed to restrict a flow to only allow
	//one event in the pending state to exist at the same time.
	//!!!
	RetrieveFlowPendingEvent(ctx context.Context, flowId string) (*event.Event, error)

	CurrentProcessorNumber(ctx context.Context) int
}

type eventScheduler struct {

	//name scheduler name.
	//reserved
	name string

	//maxProcessor limit the number of processor goroutine per host
	maxProcessor int

	//processors is directory of processor which are used to process event
	processors map[string]Processor
}

func (s *eventScheduler) Schedule(ctx context.Context) error {
	//TODO ctx分离
	go s.doSchedule(ctx)
	return nil
}

func (s *eventScheduler) doSchedule(ctx context.Context) {

}

func (s *eventScheduler) RetrievePendingEvent(ctx context.Context) (*event.Event, error) {
	return nil, nil
}

func (s *eventScheduler) RetrieveExpiredEvent(ctx context.Context) (*event.Event, error) {
	return nil, nil
}

func (s *eventScheduler) RetrieveFlowPendingEvent(ctx context.Context, flowId string) (*event.Event, error) {
	return adapter.RetrieveFlowPendingEvent(ctx, flowId)
}

// startProcessing starts the event processing loop for all processors
func (s *eventScheduler) startProcessing(ctx context.Context) {
	return
}

func (s *eventScheduler) CurrentProcessorNumber(ctx context.Context) int {
	return len(s.processors)
}
