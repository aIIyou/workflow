package schedule

import (
	"context"
	"sync/atomic"

	"github.com/aIIyou/workflow/event"
)

var (
	activateWorker atomic.Int64
)

func InitProcessor() {
	var initCount int64 = 0
	atomic.LoadInt64(&initCount)
}

// Processor defines the interface for event scheduling operations in the workflow system.
// It provides methods for retrieving, processing, and creating events in the scheduling pipeline.
type Processor interface {

	// CreateNextEvent creates the next event in the workflow sequence based on the current event.
	// This method should determine what event should follow the current one and create it in the system.
	// Parameters:
	//   - e: The current event that has been processed
	// Returns:
	//   - error: Any error encountered during creation, such as invalid event data or storage issues
	CreateNextEvent(ctx context.Context, e *event.Event) error

	//GetProcessorId return the processor unique identification
	GetProcessorId(ctx context.Context) string

	Process(ctx context.Context)
}

// defaultProcessor is a basic implementation of Processor interface
type defaultProcessor struct{}

func (p *defaultProcessor) RetrievePendingEvent(ctx context.Context) (*event.Event, error) {
	// TODO: Implement event retrieval logic
	return nil, nil
}

func (p *defaultProcessor) ProcessPendingEvent(ctx context.Context, e *event.Event) error {
	// TODO: Implement event processing logic
	return nil
}

func (p *defaultProcessor) CreateNextEvent(ctx context.Context, e *event.Event) error {
	// TODO: Implement next event creation logic
	return nil
}

func (p *defaultProcessor) GetProcessorId(ctx context.Context) string {
	// TODO: Implement processor ID retrieval
	return "default_processor"
}

func (p *defaultProcessor) Process(ctx context.Context) {

}
