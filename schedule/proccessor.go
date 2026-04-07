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
	"github.com/aIIyou/workflow/storage/adapter"
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

	ProcessAsyncPendingEvent(ctx context.Context, event *event.Event) error

	ProcessSyncPendingEvent(ctx context.Context, event *event.Event) error

	Process(ctx context.Context, event *event.Event)
}

// defaultProcessor is a basic implementation of Processor interface
type defaultProcessor struct {
	Name string
	IP   string
}

func (p *defaultProcessor) ProcessAsyncPendingEvent(ctx context.Context, e *event.Event) error {
	flowName := e.FlowName
	eventName := e.Name
	flowId := e.FlowId
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

	//获取flow中的data,种入ctx中
	eventFlowInstance, err := adapter.RetrieveEventFlowInstance(ctx, flowId)
	if err != nil {
		return err
	}
	data := eventFlowInstance.Data
	//将data反序列化（这里没有结构体参照，直接用通用的反序列化),然后种入到ctx中，key名为flow.KeyBusinessData
	var businessData map[string]interface{}
	if err := json.Unmarshal([]byte(data), &businessData); err != nil {
		return fmt.Errorf("failed to unmarshal business data: %v", err)
	}
	ctx = context.WithValue(ctx, flow.KeyBusinessData, businessData)

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

	return p.CreateNextEvent(ctx, e, eventFlow)
}

func (p *defaultProcessor) ProcessSyncPendingEvent(ctx context.Context, e *event.Event) error {
	return nil
}

func (p *defaultProcessor) CreateNextEvent(ctx context.Context, e *event.Event, ef *flow.FLow) error {
	// TODO: Implement next event creation logic
	return nil
}

func (p *defaultProcessor) GetProcessorId(ctx context.Context) string {
	// TODO: Implement processor ID retrieval
	return "default_processor"
}

func (p *defaultProcessor) Process(ctx context.Context, e *event.Event) {

}

func NewProcessor() Processor {

	now := time.Now().String()
	localIP := util.BoundMachineUtil{}.LocalIP()
	return &defaultProcessor{
		Name: fmt.Sprintf(`%s_%s`, localIP, now),
		IP:   localIP,
	}
}
