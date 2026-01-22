package schedule

import "context"

var (
	// scheduler global event process scheduler
	// it is guaranteed that here is only one scheduler in os specified process
	scheduler Scheduler
)

// ScheduleInit
// avoid to use golang init function as initialization sequence out of control
func ScheduleInit() {
	return
}

type Scheduler interface {

	//Schedule launch entry of Scheduler
	Schedule(ctx context.Context) error

	SchedulePendingEvent(ctx context.Context) error

	ScheduleInactiveEvent(ctx context.Context) error
}

type eventScheduler struct {

	//name scheduler name.
	//reserved
	name string

	//maxProcessor limit the number of processor goroutine per host
	maxProcessor int
}
