package schedule

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aIIyou/workflow/event"
	"github.com/aIIyou/workflow/storage/adapter"
)

var (
	// globalScheduler global event process globalScheduler
	// it is guaranteed that here is only one globalScheduler in os specified process
	globalScheduler Scheduler

	//schedulerOnce globalScheduler could be initialized only once
	schedulerOnce sync.Once

	maintain atomic.Bool
)

const (
	eventSchedulerName = "event_scheduler"
)

// ScheduleInit
// avoid to use golang init function as initialization sequence out of control
func ScheduleInit(maxProcessor int64) {
	f := func() {
		globalScheduler = &eventScheduler{
			name:          eventSchedulerName,
			maxProcessor:  maxProcessor,
			processors:    make(map[string]Processor, maxProcessor),
			syncProcessor: new(defaultProcessor),
		}
	}
	schedulerOnce.Do(f)
	maintain.Store(false)
	return
}

func LaunchScheduler(ctx context.Context) error {
	return globalScheduler.Schedule(ctx)
}

func CurrentProcessNumber(ctx context.Context) int64 {
	return globalScheduler.CurrentProcessorNumber(ctx)
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

	CurrentProcessorNumber(ctx context.Context) int64
}

type eventScheduler struct {

	//name globalScheduler name.
	//reserved
	name string

	//maxProcessor limit the number of processor goroutine per host
	maxProcessor int64

	//curProcessor records current process quant
	curProcessor atomic.Int64

	//processors is directory of processor which are used to process asynchronous event
	processors map[string]Processor

	//syncProcessor is the processor which is used to process synchronous event
	syncProcessor Processor
}

func (s *eventScheduler) Schedule(ctx context.Context) error {
	//TODO ctx分离
	go s.doSchedule(ctx)
	return nil
}

func (s *eventScheduler) doSchedule(ctx context.Context) {
	go s.PendingEventLoop(ctx)
	go s.ExpiredEventLoop(ctx)
}

func (s *eventScheduler) PendingEventLoop(ctx context.Context) {
	for {
		if maintain.Load() {
			time.Sleep(time.Second)
		}
		if s.curProcessor.Load() >= s.maxProcessor {
			time.Sleep(time.Second)
		}
		pendingEvent, err := s.RetrievePendingEvent(ctx)
		if err != nil {
			time.Sleep(time.Second)
		}
		processor := NewProcessor()

		go processor.Process(context.Background(), pendingEvent)
	}
}

func (s *eventScheduler) RetrievePendingEvent(ctx context.Context) (*event.Event, error) {

	pendingEvent, err := adapter.RetrievePendingEvent(ctx)
	if err != nil {
		return nil, err
	}

	return event.NewFromModel(pendingEvent), nil
}

func (s *eventScheduler) ExpiredEventLoop(ctx context.Context) {
	for {
		if maintain.Load() {
			time.Sleep(time.Second)
		}
		if s.curProcessor.Load() >= s.maxProcessor {
			time.Sleep(time.Second)
		}
		expiredEvent, err := s.RetrieveExpiredEvent(ctx)
		if err != nil {
			time.Sleep(time.Second)
		}
		processor := NewProcessor()

		go processor.Process(context.Background(), expiredEvent)
	}
}

func (s *eventScheduler) RetrieveExpiredEvent(ctx context.Context) (*event.Event, error) {
	pendingEvent, err := adapter.RetrieveExpiredEvent(ctx)
	if err != nil {
		return nil, err
	}
	return event.NewFromModel(pendingEvent), nil
}

func (s *eventScheduler) CurrentProcessorNumber(ctx context.Context) int64 {
	return s.curProcessor.Load()
}
