package schedule

import (
	"context"
	"sync"

	"github.com/aIIyou/workflow/event"
)

var (
	// scheduler global event process scheduler
	// it is guaranteed that here is only one scheduler in os specified process
	scheduler Scheduler

	//schedulerOnce scheduler could be initialized only once
	schedulerOnce sync.Once
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
	return
}

func LaunchScheduler(ctx context.Context) error {
	return scheduler.Schedule(ctx)
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
	return nil
}

func (s *eventScheduler) RetrievePendingEvent(ctx context.Context) (*event.Event, error) {
	return nil, nil
}

func (s *eventScheduler) RetrieveExpiredEvent(ctx context.Context) (*event.Event, error) {
	return nil, nil
}

// startProcessing starts the event processing loop for all processors
func (s *eventScheduler) startProcessing(ctx context.Context) {
	return
}
