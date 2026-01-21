package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// 子进程执行的结果
type ProcessResult struct {
	Index    int           `json:"index"`
	FlowID   int64         `json:"flow_id"`
	Duration time.Duration `json:"duration"`
	Error    string        `json:"error,omitempty"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		// 子进程模式
		worker()
		return
	}

	// 主进程模式
	master()
}

// 子进程函数 - 执行实际的数据库操作
func worker() {
	// 解析参数
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s worker <index>\n", os.Args[0])
		os.Exit(1)
	}

	var index int
	fmt.Sscanf(os.Args[2], "%d", &index)

	result := ProcessResult{Index: index}

	// 建立数据库连接
	db, err := sql.Open("mysql", "root:Nwpuyaoxin94.@tcp(9.134.245.207:3306)/my_workflow")
	if err != nil {
		result.Error = fmt.Sprintf("数据库连接错误: %v", err)
		outputResult(result)
		return
	}
	defer db.Close()

	// 设置连接池参数
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(time.Minute * 5)

	startTime := time.Now()

	var flowId int64
	tx, txErr := db.Begin()
	if txErr != nil {
		result.Error = fmt.Sprintf("事务开始错误: %v", txErr)
		outputResult(result)
		return
	}

	rows, txErr := tx.Query("select `id` from work_flow where status = 'pending' limit 1 for update ")
	if txErr != nil {
		result.Error = fmt.Sprintf("查询错误: %v", txErr)
		outputResult(result)
		return
	}

	if rows.Next() {
		txErr = rows.Scan(&flowId)
		if txErr != nil {
			result.Error = fmt.Sprintf("扫描错误: %v", txErr)
			rows.Close()
			outputResult(result)
			return
		}
		rows.Close()
	}

	_, txErr = tx.Exec("UPDATE work_flow SET status = 'processing' WHERE id = ?", flowId)
	if txErr != nil {
		result.Error = fmt.Sprintf("更新错误: %v", txErr)
		outputResult(result)
		return
	}

	txErr = tx.Commit()
	if txErr != nil {
		result.Error = fmt.Sprintf("提交错误: %v", txErr)
		outputResult(result)
		return
	}

	result.FlowID = flowId
	result.Duration = time.Since(startTime)
	outputResult(result)
}

// 输出结果给主进程
func outputResult(result ProcessResult) {
	jsonData, _ := json.Marshal(result)
	fmt.Println(string(jsonData))
}

// 主进程函数 - 启动和管理子进程
func master() {
	numProcesses := 100
	var wg sync.WaitGroup
	results := make(chan ProcessResult, numProcesses)
	var maxDuration time.Duration = -1

	// 启动子进程
	for i := 0; i < numProcesses; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// 启动子进程
			cmd := exec.Command(os.Args[0], "worker", fmt.Sprintf("%d", index))

			// 获取子进程输出
			output, err := cmd.Output()
			if err != nil {
				fmt.Printf("进程 %d 执行错误: %v\n", index, err)
				results <- ProcessResult{
					Index:    index,
					Error:    err.Error(),
					Duration: time.Duration(0),
				}
				return
			}

			// 解析子进程输出
			var result ProcessResult
			if err := json.Unmarshal(output, &result); err != nil {
				fmt.Printf("进程 %d 输出解析错误: %v\n", index, err)
				results <- ProcessResult{
					Index:    index,
					Error:    err.Error(),
					Duration: time.Duration(0),
				}
				return
			}

			results <- result
		}(i)
	}

	// 等待所有子进程完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	for result := range results {
		if result.Error != "" {
			fmt.Printf("进程 %d 失败: %s\n", result.Index, result.Error)
		} else {
			fmt.Printf("进程 %d, Flow ID: %d, 耗时: %v\n", result.Index, result.FlowID, result.Duration)
			if result.Duration > maxDuration {
				maxDuration = result.Duration
			}
		}
	}

	fmt.Printf("最大等待时长: %v\n", maxDuration)
}
