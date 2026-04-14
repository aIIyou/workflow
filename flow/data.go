package flow

import (
	"fmt"
	"time"

	"github.com/aIIyou/workflow/exec"
)

func RetrieveVisibleAt(data any) (visibleAt *time.Time, err error) {
	dataMap, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(`data is not a map[string]any`)
	}
	if executeType, existed := dataMap[exec.ExecuteType]; existed {
		switch executeType {
		case exec.ExecuteTypeAuto:
			return nil, nil
		case exec.ExecuteTypeManual:
			maxTime := &exec.MaxTime
			return maxTime, nil
		case exec.ExecuteTypeTimed:
			if t, existed := dataMap[exec.TimedTime]; existed {
				switch t.(type) {
				case string:
					tt, err := time.Parse("2006-01-02 15:04:05 ", t.(string))
					if err != nil {
						return nil, err
					}
					return &tt, nil
				case time.Time:
					tt := t.(time.Time)
					return &tt, nil
				default:
					return nil, fmt.Errorf(`timed_time not string or time.Time`)
				}
			} else {
				return nil, fmt.Errorf(`timed_time not configured`)
			}
		default:
			return nil, fmt.Errorf(`execute type not support`)
		}
	} else {
		return nil, nil
	}
}
