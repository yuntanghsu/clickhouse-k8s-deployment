package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"

	"github.com/ClickHouse/clickhouse-go"
	"k8s.io/klog/v2"
)

// var availability []int
var recordPerCommit, commitNum, insertInterval, memorySize int
var availableTime, totalTime int
var host string

// log results when the program is interupted
func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		// writeToFile(availability)
		logResult()
		os.Exit(0)
	}()
}

func createClickHouseClient() *sql.DB {
	hostAddress := fmt.Sprintf("tcp://%s:9000?debug=true&username=clickhouse_operator&password=clickhouse_operator_password", host)
	connect, err := sql.Open("clickhouse", hostAddress)
	if err != nil {
		klog.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil
	}
	return connect
}

func getRandIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}

func addFakeRecord(stmt *sql.Stmt) {
	if _, err := stmt.Exec(
		time.Now(),
		time.Now(),
		time.Now(),
		time.Now(),
		time.Now(),
		0,
		getRandIP(),
		getRandIP(),
		uint16(rand.Intn(65535)),
		uint16(rand.Intn(65535)),
		6,
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		fmt.Sprintf("PodName-%d", rand.Int()),
		fmt.Sprintf("PodNameSpace-%d", rand.Int()),
		fmt.Sprintf("NodeName-%d", rand.Int()),
		fmt.Sprintf("PodName-%d", rand.Int()),
		fmt.Sprintf("PodNameSpace-%d", rand.Int()),
		fmt.Sprintf("NodeName-%d", rand.Int()),
		getRandIP(),
		uint16(rand.Intn(65535)),
		fmt.Sprintf("ServicePortName-%d", rand.Int()),
		fmt.Sprintf("PolicyName-%d", rand.Int()),
		fmt.Sprintf("PolicyNameSpace-%d", rand.Int()),
		fmt.Sprintf("PolicyRuleName-%d", rand.Int()),
		1,
		1,
		fmt.Sprintf("PolicyName-%d", rand.Int()),
		fmt.Sprintf("PolicyNameSpace-%d", rand.Int()),
		fmt.Sprintf("PolicyRuleName-%d", rand.Int()),
		1,
		1,
		"tcpState",
		0,
		fmt.Sprintf("PodLabels-%d", rand.Int()),
		fmt.Sprintf("PodLabels-%d", rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
		uint64(rand.Int()),
	); err != nil {
		klog.Fatal(err)
	}
}

func writeRecords(connect *sql.DB) {

	for i := 0; i < commitNum; i++ {
		var (
			tx, _   = connect.Begin()
			stmt, _ = tx.Prepare("INSERT INTO flows (timeInserted,flowStartSeconds,flowEndSeconds,flowEndSecondsFromSourceNode,flowEndSecondsFromDestinationNode,flowEndReason,sourceIP,destinationIP,sourceTransportPort,destinationTransportPort,protocolIdentifier,packetTotalCount,octetTotalCount,packetDeltaCount,octetDeltaCount,reversePacketTotalCount,reverseOctetTotalCount,reversePacketDeltaCount,reverseOctetDeltaCount,sourcePodName,sourcePodNamespace,sourceNodeName,destinationPodName,destinationPodNamespace,destinationNodeName,destinationClusterIP,destinationServicePort,destinationServicePortName,ingressNetworkPolicyName,ingressNetworkPolicyNamespace,ingressNetworkPolicyRuleName,ingressNetworkPolicyRuleAction,ingressNetworkPolicyType, egressNetworkPolicyName,egressNetworkPolicyNamespace,egressNetworkPolicyRuleName,egressNetworkPolicyRuleAction,egressNetworkPolicyType,tcpState,flowType,sourcePodLabels,destinationPodLabels,throughput,reverseThroughput,throughputFromSourceNode,throughputFromDestinationNode,reverseThroughputFromSourceNode,reverseThroughputFromDestinationNode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		)
		defer stmt.Close()
		for j := 0; j < recordPerCommit; j++ {
			addFakeRecord(stmt)
		}
		fmt.Println(i)
		if err := tx.Commit(); err != nil {
			fmt.Printf("Error: %v", err)
			// availability = append(availability, 0)
		} else {
			availableTime += 1
			// availability = append(availability, 1)
		}
		totalTime += 1
		time.Sleep(time.Duration(insertInterval) * time.Second)
	}
	// plotAvailability(availability, commitNum)
	// writeToFile(availability)
	logResult()
}

func logResult() {
	f, err := os.OpenFile("test.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		klog.Error(err)
	}
	defer f.Close()
	result := fmt.Sprintf("memory_size=%dg, batch_size=%d, batch_frequecy=%ds, insert_rate=%d, availibility=%f, duration=%d\n", memorySize, recordPerCommit, insertInterval, recordPerCommit/insertInterval, float32(availableTime)/float32(totalTime), totalTime*insertInterval)
	if _, err := f.WriteString(result); err != nil {
		klog.Error(err)
	}
}

func main() {
	// example: write 1,000 records in a batch, 1800 writes in total,
	// insertion interval at 1s, log with memory size 2G and connect to clickhouse host at 127.0.0.1
	// go run . -r 1000 -c 1800 -i 1 -m 2 -h 127.0.0.1
	flag.IntVar(&recordPerCommit, "r", 1, "records number per commit")
	flag.IntVar(&commitNum, "c", 1, "commits number")
	flag.IntVar(&insertInterval, "i", 1, "insertion interval")
	flag.IntVar(&memorySize, "m", 1, "memory size(Gb)")
	flag.StringVar(&host, "h", "localhost", "Clickhouse address")
	flag.Parse()

	connect := createClickHouseClient()

	SetupCloseHandler()
	writeRecords(connect)

}

// Helpful function not used in performance test
func writeToFile(data []int) {
	fileName := fmt.Sprintf("data_4g_%d_%ds.csv", recordPerCommit, insertInterval)
	f, err := os.Create(fileName)
	if err != nil {
		klog.Error(err)
	}
	defer f.Close()
	for i, value := range data {
		f.WriteString(fmt.Sprintf("%d, %d\n", i, value))
	}
}

func plotAvailability(data []int, n int) {
	p := plot.New()
	p.Title.Text = "Availability recording time"
	p.X.Label.Text = "time(second)"
	p.Y.Label.Text = "Availability"

	pts := make(plotter.XYs, n)
	for i := range pts {
		pts[i].X = float64(i)
		pts[i].Y = float64(data[i])
	}

	scatterPlot, err := plotter.NewScatter(pts)
	if err != nil {
		panic(err)
	}
	p.Add(scatterPlot)
	if err := p.Save(4*vg.Inch, 4*vg.Inch, "availability.png"); err != nil {
		panic(err)
	}

	// s.GlyphStyle.Color = color.RGBA{R: 255, B: 128, A: 255}

}

func setTTLMergeTimeout(connect *sql.DB) {
	if _, err := connect.Exec("ALTER TABLE default.antrea MODIFY SETTING merge_with_ttl_timeout = 600"); err != nil {
		klog.Fatal(err)
	}
}
