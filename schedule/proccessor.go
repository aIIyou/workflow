package schedule

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/aIIyou/workflow/event"
	"github.com/aIIyou/workflow/flow"
	"github.com/aIIyou/workflow/util"
)

var (
	activateProcessor atomic.Int64
)

var (
	maxProcessor int64 = 20
)

func InitProcessor() {
	activateProcessor.Store(0)
}

// Processor defines the interface for event scheduling operations in the workflow system.
// It provides methods for retrieving, processing, and creating events in the scheduling pipeline.
type Processor interface {

	//// CreateNextEvent creates the next event in the workflow sequence based on the current event.
	//// This method should determine what event should follow the current one and create it in the system.
	//// Parameters:
	////   - e: The current event that has been processed
	//// Returns:
	////   - error: Any error encountered during creation, such as invalid event data or storage issues
	//CreateNextEvent(ctx context.Context, e *event.Event) error

	//GetProcessorId return the processor unique identification
	GetProcessorId(ctx context.Context) string

	//processAsyncPendingEvent(ctx context.Context, event *event.Event) error
	//
	//processSyncPendingEvent(ctx context.Context, event *event.Event) error

	Process(ctx context.Context, event *event.Event)
}

// defaultProcessor is a basic implementation of Processor interface
type defaultProcessor struct {
	ID        string
	IP        string
	scheduler Scheduler
}

func (p *defaultProcessor) executeUserMethod(ctx context.Context, e *event.Event) error {
	flowId := e.FlowId
	flowName := e.FlowName
	eventName := e.Name
	eventFlow, err := flow.RetrieveEventflow(flowName)
	if err != nil {
		return err
	}
	handler := eventFlow.Handler()

	// 使用反射的方式，找到handler这个结构体上定义的eventName方法
	handlerValue := reflect.ValueOf(handler)
	if handlerValue.Kind() == reflect.Ptr {
		handlerValue = handlerValue.Elem()
	}

	// 查找方法
	method := handlerValue.MethodByName(eventName)
	if !method.IsValid() {
		return fmt.Errorf("method %s not found in handler", eventName)
	}

	//获取用户写入的控制变量，因为用户的事务已经提交了，这里直接查数据表是可以获取到最新的控制变量的
	f, err := (&flow.EventFlowInstance{}).RetrieveEventFlowInstance(ctx, flowId)
	if err != nil {
		return err
	}
	flowDataStr, err := f.RetrieveEventFlowData(ctx)
	if err != nil {
		return err
	}
	var flowData map[string]interface{}
	if err := json.Unmarshal([]byte(flowDataStr), &flowData); err != nil {
		return fmt.Errorf("failed to unmarshal business data: %v", err)
	}
	ctx = context.WithValue(ctx, flow.KeyData, flowData)

	// 调用方法，传入context参数
	results := method.Call([]reflect.Value{
		reflect.ValueOf(ctx),
	})

	// 检查是否有错误返回
	if len(results) > 0 {
		if errVal, ok := results[0].Interface().(error); ok && errVal != nil {
			return errVal
		}
	}
	return nil
}

func (p *defaultProcessor) processAsyncPendingEvent(ctx context.Context, e *event.Event) error {
	err := p.executeUserMethod(ctx, e)
	if err != nil {
		return err
	}

	//TODO 这里要交给EventFlow去流转了
	return nil
}

func (p *defaultProcessor) processSyncPendingEvent(ctx context.Context, e *event.Event) error {
	return nil
}

func (p *defaultProcessor) processAsyncExpiredEvent(ctx context.Context, e *event.Event) error {
	return nil
}

func (p *defaultProcessor) GetProcessorId(ctx context.Context) string {
	return p.ID
}

func (p *defaultProcessor) Process(ctx context.Context, e *event.Event) {
	if e.Async && e.Status == event.StatusPending {
		_ = p.processAsyncPendingEvent(ctx, e)
	} else if !e.Async && e.Status == event.StatusPending {
		_ = p.processSyncPendingEvent(ctx, e)
	} else if e.Async && e.Status == event.StatusProcessing {
		_ = p.processAsyncExpiredEvent(ctx, e)
	}
}

func NewProcessor() Processor {

	now := time.Now().String()
	localIP := util.BoundMachineUtil{}.LocalIP()
	return &defaultProcessor{
		ID:        fmt.Sprintf(`%s_%s`, localIP, now),
		IP:        localIP,
		scheduler: globalScheduler,
	}
}
