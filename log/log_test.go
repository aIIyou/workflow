package log

import (
	"context"
	"testing"

	"github.com/aIIyou/workflow/flow"
)

func TestLogFunctions(t *testing.T) {
	// 测试标准输出
	InitLogger("")
	defer CloseLogger()

	// 创建带有flowId和eventId的context
	ctx := context.WithValue(context.Background(), flow.KeyData, map[string]interface{}{
		"flowId":  "test-flow-123",
		"eventId": "event-456",
	})

	// 测试各个日志级别
	Infof(ctx, "这是一条信息日志: %s", "测试信息")
	Errorf(ctx, "这是一条错误日志: %s", "测试错误")
	Debugf(ctx, "这是一条调试日志: %s", "测试调试")
	Warningf(ctx, "这是一条警告日志: %s", "测试警告")

	// 测试没有flowId和eventId的情况
	emptyCtx := context.Background()
	Infof(emptyCtx, "这是一条没有flowId和eventId的日志")
}

func TestFileLogging(t *testing.T) {
	// 测试文件输出，使用绝对路径
	logPath := "/Users/cloud/project/workflow/logs/test.log"
	err := InitLogger(logPath)
	if err != nil {
		t.Fatalf("初始化文件日志失败: %v", err)
	}
	defer CloseLogger()

	ctx := context.WithValue(context.Background(), flow.KeyData, map[string]interface{}{
		"flowId":  "file-flow-789",
		"eventId": "event-012",
	})

	Infof(ctx, "这条日志应该写入文件")
	t.Logf("日志已写入文件，请检查 %s", logPath)
}
