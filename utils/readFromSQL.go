package utils

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

// 从数据库中读取数据
type SqlData struct {
	Key   string
	Value string
}

func Start() []SqlData {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/wr")
	if err != nil {

		return nil
	}
	defer db.Close()
	rows, err := db.Query("select keyVal , value from startData")
	if err != nil {

		return nil
	}
	var q []SqlData
	defer rows.Close()
	for rows.Next() {
		var key string
		var value string
		if err = rows.Scan(&key, &value); err != nil {
			continue
		}
		q = append(q, SqlData{key, value})
	}
	return q
}
