package schedule

import (
	"context"

	"github.com/aIIyou/workflow/event"
)

// Scheduler defines the interface for event scheduling operations in the workflow system.
// It provides methods for retrieving, processing, and creating events in the scheduling pipeline.
type Scheduler interface {
	// RetrievePendingEvent retrieves the next pending event from the event queue.
	// This method should return the oldest pending event that is ready for processing.
	// Returns:
	//   - *event.Event: The retrieved pending event, or nil if no pending events are available
	//   - error: Any error encountered during retrieval, such as database connection issues
	RetrievePendingEvent(ctx context.Context) (*event.Event, error)

	// ProcessPendingEvent processes a pending event according to the workflow logic.
	// This method should handle the business logic for the event and update its status accordingly.
	// Parameters:
	//   - e: The event to be processed
	// Returns:
	//   - error: Any error encountered during processing, such as validation failures or execution errors
	ProcessPendingEvent(ctx context.Context, e *event.Event) error

	// CreateNextEvent creates the next event in the workflow sequence based on the current event.
	// This method should determine what event should follow the current one and create it in the system.
	// Parameters:
	//   - e: The current event that has been processed
	// Returns:
	//   - error: Any error encountered during creation, such as invalid event data or storage issues
	CreateNextEvent(ctx context.Context, e *event.Event) error
}
