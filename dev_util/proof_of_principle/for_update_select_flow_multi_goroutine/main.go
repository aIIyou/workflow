package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	db, err := sql.Open("mysql", "root:Nwpuyaoxin94.@tcp(9.134.245.207:3306)/my_workflow")
	if err != nil {
		panic(err)
	}
	// 设置连接池参数
	db.SetMaxOpenConns(2000)               // 最大打开连接数
	db.SetMaxIdleConns(2000)               // 最大空闲连接数
	db.SetConnMaxLifetime(time.Hour)       // 连接最大生命周期
	db.SetConnMaxIdleTime(time.Minute * 5) // 连接最大空闲时间
	defer db.Close()

	preheatConnections(db, 2000)

	wg := sync.WaitGroup{}
	var maxDuration time.Duration = -1
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var flowId int64
			tx, txErr := db.Begin()
			if txErr != nil {
				fmt.Printf("index: %d transaction begin error: %v\n", index, txErr)
				return
			}
			fmt.Printf("index: %d transaction begin successfully\n", index)
			startTime := time.Now()
			rows, txErr := tx.Query("select `id` from work_flow where status = 'pending' limit 1 for update ")
			if txErr != nil {
				fmt.Printf("index: %d query error: %v\n", index, txErr)
				return
			}
			if rows.Next() {
				txErr = rows.Scan(&flowId)
				if txErr != nil {
					fmt.Printf("index: %d scan error: %v\n", index, txErr)
					return
				}
				rows.Close()
			}
			fmt.Printf("index: %d, flow id : %+v\n", index, flowId)
			_, txErr = tx.Exec("UPDATE work_flow SET status = 'processing' WHERE id = ?", flowId)
			if txErr != nil {
				fmt.Printf("index: %d update error: %v\n", index, txErr)
				return
			}
			fmt.Printf("index: %d update successfully\n", index)
			txErr = tx.Commit()
			if txErr != nil {
				fmt.Printf("index: %d commit error: %v\n", index, txErr)
				return
			}
			duration := time.Since(startTime)
			fmt.Printf("index: %d, duration: %v\n", index, duration)
			if duration > maxDuration {
				maxDuration = duration
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("最大等待时长: %v\n", maxDuration)
}

func preheatConnections(db *sql.DB, count int) {
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(context.Background())
			if err == nil {
				conn.Close()
			}
		}()
	}
	wg.Wait()
	fmt.Printf("预热了 %d 个连接\n", count)
}
