package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"k8s.io/klog/v2"
)

var countSeries []uint64

func logFunc(connect *sql.DB) {
	rows, err := connect.Query("SELECT COUNT() FROM default.flows")
	if err != nil {
		klog.Fatal(err)
	}
	defer rows.Close()

	// only one row under default k8s deployment
	for rows.Next() {
		var (
			count uint64
		)
		if err := rows.Scan(&count); err != nil {
			klog.Fatal(err)
		}
		fmt.Printf("[clickhouse][#records] %d\n", count)
		countSeries = append(countSeries, count)
	}
}

func writeToFile(data []uint64) {
	f, err := os.Create("count.csv")
	if err != nil {
		klog.Fatal(err)
	}
	defer f.Close()
	for i, value := range data {
		f.WriteString(fmt.Sprintf("%d, %d\n", i, value))
	}
}

func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		writeToFile(countSeries)
		os.Exit(0)
	}()
}

func main() {
	connect, err := sql.Open("clickhouse", "tcp://localhost:9000?debug=true&username=clickhouse_operator&password=clickhouse_operator_password")
	if err != nil {
		klog.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
	}
	SetupCloseHandler()

	logTicker := time.NewTicker(10 * time.Second)
	for {
		<-logTicker.C
		go logFunc(connect)
	}

}
