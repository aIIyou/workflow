package log

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aIIyou/workflow/flow"
	"github.com/sirupsen/logrus"
)

var (
	logger     *logrus.Logger
	logFile    *os.File
	logToFile  bool
	logFileDir string = "./logs"
)

// InitLogger 初始化日志配置
func InitLogger(filePath string) error {
	logger = logrus.New()

	// 设置日志格式
	logrus.SetFormatter(&PlainFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
	})

	if filePath != "" {
		logToFile = true
		logFileDir = filepath.Dir(filePath)

		// 创建日志目录
		if err := os.MkdirAll(logFileDir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %v", err)
		}

		// 打开日志文件
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %v", err)
		}

		logFile = file
		logger.SetOutput(file)
	} else {
		logToFile = false
		logger.SetOutput(os.Stdout)
	}

	return nil
}

// CloseLogger 关闭日志文件
func CloseLogger() {
	if logFile != nil {
		logFile.Close()
	}
}

// Infof 输出信息级别日志
func Infof(ctx context.Context, format string, args ...interface{}) {
	logWithLevel(ctx, "INFO", format, args...)
}

// Errorf 输出错误级别日志
func Errorf(ctx context.Context, format string, args ...interface{}) {
	logWithLevel(ctx, "ERROR", format, args...)
}

// Debugf 输出调试级别日志
func Debugf(ctx context.Context, format string, args ...interface{}) {
	logWithLevel(ctx, "DEBUG", format, args...)
}

// Warningf 输出警告级别日志
func Warningf(ctx context.Context, format string, args ...interface{}) {
	logWithLevel(ctx, "WARN", format, args...)
}

// logWithLevel 统一的日志输出函数
func logWithLevel(ctx context.Context, level, format string, args ...interface{}) {
	if logger == nil {
		// 如果没有初始化，使用标准输出
		InitLogger("")
	}

	// 从context中获取data map
	var flowID, eventID string

	if dataVal := ctx.Value(flow.KeyData); dataVal != nil {
		if dataMap, ok := dataVal.(map[string]interface{}); ok {
			// 提取flowId
			if flowVal, exists := dataMap["flowId"]; exists {
				if id, ok := flowVal.(string); ok {
					flowID = id
				}
			}
			// 提取eventId
			if eventVal, exists := dataMap["eventId"]; exists {
				if id, ok := eventVal.(string); ok {
					eventID = id
				}
			}
		}
	}

	// 构建日志消息
	message := fmt.Sprintf(format, args...)

	// 构建格式化日志消息
	formattedMessage := fmt.Sprintf("{%s} {%s} %s", flowID, eventID, message)

	// 根据级别输出日志
	switch level {
	case "INFO":
		logger.Info(formattedMessage)
	case "ERROR":
		logger.Error(formattedMessage)
	case "DEBUG":
		logger.Debug(formattedMessage)
	case "WARN":
		logger.Warn(formattedMessage)
	}
}
