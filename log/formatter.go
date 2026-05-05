package log

import (
	"fmt"

	"github.com/aIIyou/workflow/consts"
	"github.com/sirupsen/logrus"
)

type PlainFormatter struct {
	TimestampFormat string
}

func (f *PlainFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	t := entry.Time
	level := entry.Level
	msg := entry.Message
	var (
		flowId  string
		eventId string
	)
	if id, existed := entry.Data[consts.KeyFlowId]; existed {
		if _, ok := id.(string); ok {
			flowId = id.(string)
		} else {
			flowId = "-"
		}
	}
	if id, existed := entry.Data[consts.KeyEventId]; existed {
		if _, ok := id.(string); ok {
			eventId = id.(string)
		} else {
			eventId = "-"
		}
	}

	tt := t.Format(f.TimestampFormat)

	s := fmt.Sprintf(`%s [%s] {%s} {%s} %s`, tt, level, flowId, eventId, msg)
	return []byte(s), nil
}
