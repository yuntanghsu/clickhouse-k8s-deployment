// Copyright 2022 Antrea Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

const (
	// The storage percentage at which the monitor starts to delete old records. By default, if the storage usage is larger than 50%, it starts to delete the old records.
	threshold = 0.5
	// The percentage of records in clickhouse will be deleted when the storage is above threshold.
	deletePercentage = 0.5

	// The monitor stops for 3 intervals after a deletion to wait for the Clickhouse MergeTree Engine to release memory.
	skipRoundsNum = 3
)

func main() {
	// The monitor stops working for several rounds after a deletion
	// as the release of the memory space for clickhouse MergeTree engine requires time
	if !skipRound() {
		connect, err := connectLoop()
		if err != nil {
			klog.Info(err)
			return
		}
		deleted := monitorMemory(connect)
		if deleted {
			klog.Infof("Number of rounds to be skipped: %d", skipRoundsNum)
		} else {
			klog.Info("Number of rounds to be skipped: 0")
		}
	}
}

// Checks the k8s log for the number of rounds to skip.
// Returns true when the monitor needs to skip more rounds and logs the number of rounds to skip for next time,
// Otherwise returns false.
func skipRound() bool {
	logString, err := getPodLogs()
	if err != nil {
		klog.Infof("error in finding last monitor job: %v", err)
		return false
	}
	// reads the number of rounds requires to be skipped
	logs := strings.Split(logString, "Number of rounds to be skipped: ")
	if len(logs) < 2 {
		klog.Infof("error in finding last monitor job: %v", err)
		return false
	}
	lines := strings.Split(logs[1], "\n")
	remainingRoundsNum, convErr := strconv.Atoi(lines[0])
	if convErr != nil {
		klog.Infof("error in finding last monitor job: %v", convErr)
		return false
	}
	if remainingRoundsNum > 0 {
		klog.Infof("Number of rounds to be skipped: %d", remainingRoundsNum-1)
		return true
	}
	return false
}

// Gets pod logs from the Clickhouse monitor job
func getPodLogs() (string, error) {
	var logString string
	namespace := "flow-visibility"
	podLogOpts := corev1.PodLogOptions{}
	config, err := rest.InClusterConfig()
	listOptions := metav1.ListOptions{
		LabelSelector: "app=clickhouse-monitor",
	}
	if err != nil {
		return logString, fmt.Errorf("error in getting config: %v", err)
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return logString, fmt.Errorf("error in getting access to K8S: %v", err)
	}
	// gets Clickhouse monitor pod
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), listOptions)
	if err != nil {
		return logString, fmt.Errorf("failed to list clickhouse monitor Pods: %v", err)
	}
	for _, pod := range pods.Items {
		// reads logs from the last successful pod
		if pod.Status.Phase == corev1.PodSucceeded {
			req := clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &podLogOpts)
			podLogs, err := req.Stream(context.TODO())
			if err != nil {
				return logString, fmt.Errorf("error in opening stream: %v", err)
			}
			defer podLogs.Close()

			buf := new(bytes.Buffer)
			_, err = io.Copy(buf, podLogs)
			if err != nil {
				return logString, fmt.Errorf("error in copy information from podLogs to buf: %v", err)
			}
			logString := buf.String()
			return logString, nil
		}
	}
	return logString, fmt.Errorf("failed to find a succeeded monitor")
}

// Connects to Clickhouse in a loop
func connectLoop() (*sql.DB, error) {
	// Connection to Clickhouse timeout if if fails for 1 minute.
	connectionTimeout := time.Minute
	// Retry connection to Clickhouse every 5 seconds if it fails.
	connectionWait := 5 * time.Second

	ticker := time.NewTicker(connectionWait)
	defer ticker.Stop()

	timeoutExceeded := time.After(connectionTimeout)
	for {
		select {
		case <-timeoutExceeded:
			return nil, fmt.Errorf("failed to connect to clickhouse after %s", connectionTimeout)

		case <-ticker.C:
			// TODO:  use k8s secret to store the username and password
			connect, err := sql.Open("clickhouse", "tcp://clickhouse-clickhouse.flow-visibility.svc.cluster.local:9000?debug=true&username=clickhouse_operator&password=clickhouse_operator_password")
			if err != nil {
				klog.Infof("failed to connect to clickhouse: %v", err)
			}
			if err := connect.Ping(); err != nil {
				if exception, ok := err.(*clickhouse.Exception); ok {
					klog.Infof("[%d] %s \n", exception.Code, exception.Message)
				} else {
					klog.Info(err)
				}
			} else {
				return connect, nil
			}
		}
	}
}

// Checks the memory usage in the Clickhouse, deletes records when it exceeds the threshold.
func monitorMemory(connect *sql.DB) bool {
	rows, err := connect.Query("SELECT free_space, total_space FROM system.disks")
	if err != nil {
		klog.Error(err)
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var (
			freeSpace  uint64
			totalSpace uint64
		)
		if err := rows.Scan(&freeSpace, &totalSpace); err != nil {
			klog.Error(err)
			return false
		}
		usagePercentage := float64(totalSpace-freeSpace) / float64(totalSpace)
		klog.Infof("Memory usage: total %d, used: %d, percentage: %f", totalSpace, totalSpace-freeSpace, usagePercentage)
		if usagePercentage > threshold {
			alterCommand := fmt.Sprintf("ALTER TABLE default.antrea DELETE WHERE id IN (SELECT id FROM default.antrea LIMIT %d)", getDeleteRowNum(connect))
			if _, err := connect.Exec(alterCommand); err != nil {
				klog.Info(err)
				return false
			}
			return true
		}
	}
	return false
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
