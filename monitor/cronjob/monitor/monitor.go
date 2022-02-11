package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"k8s.io/klog/v2"
)

const (
	// The limit of storage used by the clickhouse in byte. The default value is 1G.
	limitedSpace = 1024 * 1024 * 1024
	// The storage percentage at which the monitor starts to delete old records. By default, if the storage usage is larger than 50%, it starts to delete the old records.
	threshold = 0.5
	// The percentage of records in clickhouse will be deleted when the storage is above threshold.
	deletePercentage = 0.5

	// Connection to Clickhouse timeout if if fails for 5 minutes
	connectionTimeout = 5 * time.Minute
	// Retry connection to Clickhouse every 5 seconds if it fails
	connectionWait = 5 * time.Second
)

func monitorMemory(connect *sql.DB) {
	rows, err := connect.Query("SELECT total_bytes FROM system.tables WHERE database='default' AND name='antrea'")
	if err != nil {
		klog.Info(err)
	}
	defer rows.Close()

	// only one row
	for rows.Next() {
		var usedSpace uint64
		if err := rows.Scan(&usedSpace); err != nil {
			klog.Info(err)
		}
		usagePercentage := float64(usedSpace) / float64(limitedSpace)
		fmt.Printf("Memory usage: %f\n", usagePercentage)

		if usagePercentage > threshold {
			alterCommand := fmt.Sprintf("ALTER TABLE default.antrea DELETE WHERE id IN (SELECT id FROM default.antrea ORDER BY createTime LIMIT %d)", getDeleteRowNum(connect))
			if _, err := connect.Exec(alterCommand); err != nil {
				klog.Info(err)
			}
		}
	}
}

func getDeleteRowNum(connect *sql.DB) uint64 {
	rows, err := connect.Query("SELECT COUNT() FROM default.antrea")
	if err != nil {
		klog.Info(err)
	}
	defer rows.Close()
	var deleteRowNum uint64
	// only one row
	for rows.Next() {
		var count uint64
		if err := rows.Scan(&count); err != nil {
			klog.Info(err)
		}
		deleteRowNum = uint64(float64(count) * deletePercentage)
	}
	return deleteRowNum
}

func connectLoop() (*sql.DB, error) {
	ticker := time.NewTicker(connectionWait)
	defer ticker.Stop()

	timeoutExceeded := time.After(connectionTimeout)
	for {
		select {
		case <-timeoutExceeded:
			return nil, fmt.Errorf("clickhouse connection failed after %s", connectionTimeout)

		case <-ticker.C:
			connect, err := sql.Open("clickhouse", "tcp://clickhouse-clickhouse.flow-visibility.svc.cluster.local:9000?debug=true&username=clickhouse_operator&password=clickhouse_operator_password")
			if err != nil {
				klog.Info("failed to connect to clickhouse: ", err)
			}
			if err := connect.Ping(); err != nil {
				if exception, ok := err.(*clickhouse.Exception); ok {
					fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
				} else {
					fmt.Println(err)
				}
			} else {
				return connect, nil
			}
		}
	}
}

func main() {
	connect, err := connectLoop()
	if err != nil {
		klog.Fatal(err)
	}
	monitorMemory(connect)
}
