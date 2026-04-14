package exec

import "time"

const (
	ExecuteTypeAuto   = "auto"
	ExecuteTypeManual = "manual"
	ExecuteTypeTimed  = "timed"
)

const (
	ExecuteType = "execute_type"
	TimedTime   = "timed_time"
)

var (
	MaxTime = time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC)
)
