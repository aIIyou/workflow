package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/aIIyou/workflow/config"
	"github.com/aIIyou/workflow/event"
	"github.com/aIIyou/workflow/storage/adapter"
	"github.com/google/uuid"
)

type Transition struct {
	fromEvent string
	toEvent   string
	expr      string
	f         func(ctx context.Context) bool
}

// parseExpression 解析表达式并返回判断函数
func parseExpression(expr string) func(data interface{}) bool {
	// 表达式格式: json路径 操作符 值
	// 例如: "user.age > 18", "status == 'active'", "amount <= 100.0"

	// 解析操作符和值
	re := regexp.MustCompile(`^(.*?)\s*(==|!=|>|>=|<|<=)\s*(.+)$`)
	matches := re.FindStringSubmatch(expr)
	if len(matches) != 4 {
		return func(data interface{}) bool {
			fmt.Printf("表达式解析失败: %s\n", expr)
			return false
		}
	}

	jsonPath := strings.TrimSpace(matches[1])
	operator := strings.TrimSpace(matches[2])
	valueStr := strings.TrimSpace(matches[3])

	// 去除值字符串可能的引号
	if len(valueStr) > 1 && (valueStr[0] == '\'' && valueStr[len(valueStr)-1] == '\'' ||
		valueStr[0] == '"' && valueStr[len(valueStr)-1] == '"') {
		valueStr = valueStr[1 : len(valueStr)-1]
	}

	return func(data interface{}) bool {
		// 从data中根据jsonPath获取值
		actualValue, err := getValueByPath(data, jsonPath)
		if err != nil {
			return false
		}

		// 根据操作符进行比较
		return compareValues(actualValue, operator, valueStr)
	}
}

// getValueByPath 从数据中根据JSON路径获取值
func getValueByPath(data interface{}, path string) (interface{}, error) {
	if path == "" {
		return data, nil
	}

	keys := strings.Split(path, ".")
	current := data

	for _, key := range keys {
		switch v := current.(type) {
		case map[string]interface{}:
			if val, exists := v[key]; exists {
				current = val
			} else {
				return nil, fmt.Errorf("路径 %s 不存在", key)
			}
		case []interface{}:
			if index, err := strconv.Atoi(key); err == nil && index >= 0 && index < len(v) {
				current = v[index]
			} else {
				return nil, fmt.Errorf("数组索引 %s 无效", key)
			}
		default:
			return nil, fmt.Errorf("无法访问路径 %s", key)
		}
	}

	return current, nil
}

// compareValues 比较实际值和期望值
func compareValues(actual interface{}, operator, expectedStr string) bool {
	switch actualVal := actual.(type) {
	case float64:
		expected, err := strconv.ParseFloat(expectedStr, 64)
		if err != nil {
			return false
		}
		switch operator {
		case "==":
			return actualVal == expected
		case "!=":
			return actualVal != expected
		case ">":
			return actualVal > expected
		case ">=":
			return actualVal >= expected
		case "<":
			return actualVal < expected
		case "<=":
			return actualVal <= expected
		}
	case int:
		expected, err := strconv.Atoi(expectedStr)
		if err != nil {
			return false
		}
		switch operator {
		case "==":
			return actualVal == expected
		case "!=":
			return actualVal != expected
		case ">":
			return actualVal > expected
		case ">=":
			return actualVal >= expected
		case "<":
			return actualVal < expected
		case "<=":
			return actualVal <= expected
		}
	case int64:
		expected, err := strconv.ParseInt(expectedStr, 10, 64)
		if err != nil {
			return false
		}
		switch operator {
		case "==":
			return actualVal == expected
		case "!=":
			return actualVal != expected
		case ">":
			return actualVal > expected
		case ">=":
			return actualVal >= expected
		case "<":
			return actualVal < expected
		case "<=":
			return actualVal <= expected
		}
	case string:
		expected := expectedStr
		switch operator {
		case "==":
			return actualVal == expected
		case "!=":
			return actualVal != expected
		}
	case bool:
		expected, err := strconv.ParseBool(expectedStr)
		if err != nil {
			return false
		}
		switch operator {
		case "==":
			return actualVal == expected
		case "!=":
			return actualVal != expected
		}
	}

	return false
}

func NewTransition(fromEvent, toEvent, expr string) Transition {
	tran := Transition{
		fromEvent: fromEvent,
		toEvent:   toEvent,
		expr:      expr,
		f:         nil,
	}

	// 解析表达式并创建判断函数
	exprFunc := parseExpression(expr)

	tran.f = func(ctx context.Context) bool {
		businessData := ctx.Value(KeyBusinessData)
		if businessData == nil {
			return false
		}

		// 将businessData转换为map[string]interface{}格式
		var data interface{}
		switch v := businessData.(type) {
		case map[string]interface{}:
			data = v
		case string:
			var jsonData map[string]interface{}
			if err := json.Unmarshal([]byte(v), &jsonData); err == nil {
				data = jsonData
			} else {
				return false
			}
		case []byte:
			var jsonData map[string]interface{}
			if err := json.Unmarshal(v, &jsonData); err == nil {
				data = jsonData
			} else {
				return false
			}
		default:
			// 尝试JSON序列化
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return false
			}
			var jsonData map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &jsonData); err != nil {
				return false
			}
			data = jsonData
		}

		return exprFunc(data)
	}

	return tran
}

// GetFromEvent 获取源事件名称
func (t *Transition) GetFromEvent() string {
	return t.fromEvent
}

// GetToEvent 获取目标事件名称
func (t *Transition) GetToEvent() string {
	return t.toEvent
}

// GetExpression 获取表达式
func (t *Transition) GetExpression() string {
	return t.expr
}

// Evaluate 评估转换条件
func (t *Transition) Evaluate(ctx context.Context) bool {
	if t.f == nil {
		return false
	}
	return t.f(ctx)
}

type EventFlow struct {

	//_type  event flow type.
	_type string

	// event flow name.
	name string

	//events contained by event flow.
	events []string

	//first event of event flow.
	startEvent string

	//transitions define how to flow from one event to another after it is executed.
	transitions map[string][]Transition

	//handler is the event flow handler.
	handler any

	mu *sync.RWMutex
}

// NewEventFlow 创建一个新的事件流实例
func NewEventFlow(flowType, name string, events []string, transitions []Transition) *EventFlow {
	flow := &EventFlow{
		_type:       flowType,
		name:        name,
		mu:          new(sync.RWMutex),
		events:      events,
		transitions: make(map[string][]Transition),
	}
	for _, tran := range transitions {
		if _, existed := flow.transitions[tran.fromEvent]; !existed {
			flow.transitions[tran.fromEvent] = make([]Transition, 0)
		}
		flow.transitions[tran.fromEvent] = append(flow.transitions[tran.fromEvent], tran)
	}
	return flow
}

func (flow *EventFlow) Name() string {
	flow.mu.RLock()
	defer flow.mu.RUnlock()
	name := flow.name
	return name
}

func (flow *EventFlow) Type() string {
	flow.mu.RLock()
	defer flow.mu.RUnlock()
	_type := flow._type
	return _type
}

func (flow *EventFlow) AddEvents(events []string) *EventFlow {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if flow.events == nil {
		flow.events = make([]string, 0)
	}
	flow.events = append(flow.events, events...)
	return flow
}

func (flow *EventFlow) AddTransitions(transitions []config.Transition) *EventFlow {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if flow.transitions == nil {
		flow.transitions = make(map[string][]Transition)
	}
	for _, tran := range transitions {
		if _, existed := flow.transitions[tran.FromEvent]; !existed {
			flow.transitions[tran.FromEvent] = make([]Transition, 0)
		}
		flow.transitions[tran.FromEvent] = append(flow.transitions[tran.FromEvent], NewTransition(
			tran.FromEvent,
			tran.ToEvent,
			tran.Expr,
		))
	}
	return flow
}

func (flow *EventFlow) NextEvent(event *event.Event) string {
	return ""
}

var (
	globalWorkflow      map[string]*EventFlow
	globalWorkflowMutex sync.RWMutex
)

// RegisterWorkflow register workflow
func RegisterWorkflow(name string, handler any, conf *config.Configuration) error {
	globalWorkflowMutex.Lock()
	defer globalWorkflowMutex.Unlock()
	if globalWorkflow == nil {
		globalWorkflow = make(map[string]*EventFlow)
	}
	if _, existed := globalWorkflow[name]; existed {
		return fmt.Errorf(`workflow "%s" already registered`, name)
	}
	if conf == nil {
		conf = config.GetConfigure()
	}
	if conf == nil {
		return fmt.Errorf("configure is nil")
	}
	for _, flowConfig := range conf.Flow {
		if flowConfig.FlowName != name {
			continue
		}
		eventNames := make([]string, len(flowConfig.Event))
		for i, eventConfig := range flowConfig.Event {
			eventNames[i] = eventConfig.Name
		}

		if err := validateHandler(handler, eventNames); err != nil {
			return err
		}
		flow := &EventFlow{
			_type:      name,
			name:       name,
			events:     eventNames,
			startEvent: flowConfig.StartEvent,
			handler:    handler,
			mu:         &sync.RWMutex{},
		}
		flow.AddTransitions(flowConfig.Transitions)
		globalWorkflow[name] = flow
		return nil
	}
	return fmt.Errorf(`workflow "%s" not configured`, name)
}

func validateHandler(handler any, eventsName []string) error {
	// Validate that handler is a struct or pointer to struct
	val := reflect.ValueOf(handler)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("handler must be a struct or pointer to struct, got %s", val.Kind())
	}

	// Convert eventsName to camel case with first letter capitalized
	expectedMethods := make(map[string]bool)
	for _, eventName := range eventsName {
		methodName := toCamelCase(eventName)
		expectedMethods[methodName] = true
	}

	// Get all methods of the handler
	handlerType := reflect.TypeOf(handler)
	for i := 0; i < handlerType.NumMethod(); i++ {
		method := handlerType.Method(i)

		// Only validate methods that are in the expected methods list
		// Allow extra methods in the handler
		if expectedMethods[method.Name] {
			// Check method signature: parameter should be context.Context, return value should be error
			if method.Type.NumIn() != 2 { // First parameter is receiver
				return fmt.Errorf("method %s should have exactly 1 parameter (context.Context)", method.Name)
			}

			paramType := method.Type.In(1)
			if paramType.String() != "context.Context" {
				return fmt.Errorf("method %s parameter should be context.Context, got %s", method.Name, paramType)
			}

			if method.Type.NumOut() != 1 {
				return fmt.Errorf("method %s should return exactly 1 value (error)", method.Name)
			}

			returnType := method.Type.Out(0)
			if returnType.String() != "error" {
				return fmt.Errorf("method %s should return error, got %s", method.Name, returnType)
			}

			// Remove found method from expected methods list
			delete(expectedMethods, method.Name)
		}
	}

	// Check if all expected methods exist
	if len(expectedMethods) > 0 {
		return fmt.Errorf("missing methods in handler: %v", getKeys(expectedMethods))
	}

	return nil
}

// toCamelCase Convert string to camelCase notation starting with uppercase
func toCamelCase(s string) string {
	if s == "" {
		return s
	}

	//处理下划线分隔的命名
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}

	return strings.Join(parts, "")
}

// getKeys Get all keys of map
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

//event workflow control entry

func StartEventFlow(ctx context.Context, name string, data any) (flowId string, err error) {
	globalWorkflowMutex.RLock()
	defer globalWorkflowMutex.RUnlock()
	workflow, ok := globalWorkflow[name]
	if !ok {
		return "", fmt.Errorf("event flow %s not found", name)
	}

	flowId = uuid.NewString()

	startEventName := workflow.startEvent

	//event entity
	startEvent := &event.Event{
		Id:       uuid.NewString(),
		Type:     startEventName,
		Name:     startEventName,
		Status:   event.StatusPending,
		Ctx:      context.Background(),
		FlowId:   flowId,
		FlowType: name,
	}
	err = adapter.CreateEvent(ctx, startEvent)
	return flowId, err
}
