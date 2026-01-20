package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:Nwpuyaoxin94.@tcp(9.134.245.207:3306)/my_workflow")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("TRUNCATE TABLE `work_flow`")
	if err != nil {
		panic(err)
	}

	baseSql := "INSERT INTO `work_flow` (`id`,`data`,`status`) VALUES"

	data := make([]string, 0)
	for i := 1; i <= 10000; i++ {
		data = append(data, fmt.Sprintf("(%d,%s,%s)", i, fmt.Sprintf("'数据%d'", i), "'pending'"))
	}
	dataStr := strings.Join(data, ",")

	baseSql += dataStr

	_, err = db.Exec(baseSql)
	if err != nil {
		panic(err)
	}
}
