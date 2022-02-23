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

var availability []int
var recordPerCommit, commitNum, insertInterval int

func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		writeToFile(availability)
		os.Exit(0)
	}()
}

func createClickHouseClient() *sql.DB {
	connect, err := sql.Open("clickhouse", "tcp://10.110.248.227:9000?debug=true&username=clickhouse_operator&password=clickhouse_operator_password")
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

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS flows (
			timeInserted DateTime,
			flowStartSeconds DateTime,
			flowEndSeconds DateTime,
			flowEndSecondsFromSourceNode DateTime,
			flowEndSecondsFromDestinationNode DateTime,
			flowEndReason UInt8,
			sourceIP String,
			destinationIP String,
			sourceTransportPort UInt16,
			destinationTransportPort UInt16,
			protocolIdentifier UInt8,
			packetTotalCount UInt64,
			octetTotalCount UInt64,
			packetDeltaCount UInt64,
			octetDeltaCount UInt64,
			reversePacketTotalCount UInt64,
			reverseOctetTotalCount UInt64,
			reversePacketDeltaCount UInt64,
			reverseOctetDeltaCount UInt64,
			sourcePodName String,
			sourcePodNamespace String,
			sourceNodeName String,
			destinationPodName String,
			destinationPodNamespace String,
			destinationNodeName String,
			destinationClusterIP String,
			destinationServicePort UInt16,
			destinationServicePortName String,
			ingressNetworkPolicyName String,
			ingressNetworkPolicyNamespace String,
			ingressNetworkPolicyRuleName String,
			ingressNetworkPolicyRuleAction UInt8,
			ingressNetworkPolicyType UInt8,
			egressNetworkPolicyName String,
			egressNetworkPolicyNamespace String,
			egressNetworkPolicyRuleName String,
			egressNetworkPolicyRuleAction UInt8,
			egressNetworkPolicyType UInt8,
			tcpState String,
			flowType UInt8,
			sourcePodLabels String,
			destinationPodLabels String,
			throughput UInt64,
			reverseThroughput UInt64,
			throughputFromSourceNode UInt64,
			throughputFromDestinationNode UInt64,
			reverseThroughputFromSourceNode UInt64,
			reverseThroughputFromDestinationNode UInt64,
			trusted Bool
		) engine=MergeTree
		ORDER BY (timeInserted, flowEndSeconds)
		TTL timeInserted + INTERVAL 10 MINUTE
		SETTINGS merge_with_ttl_timeout = 600;
	`)
	if err != nil {
		klog.Fatal(err)
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

func writeRecords(recordPerCommit, commitNum int, insertInterval int, connect *sql.DB) {

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
			availability = append(availability, 0)
		} else {
			availability = append(availability, 1)
		}
		// wait 1 second before next write
		time.Sleep(time.Duration(insertInterval) * time.Second)
	}
	// plotAvailability(availability, commitNum)
	writeToFile(availability)
}

func writeToFile(data []int) {
	fileName := fmt.Sprintf("data_8g_%d_%ds.csv", recordPerCommit, insertInterval)
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

func main() {
	// example: write 100,000 records at a time, 20 writes in total
	// go run . -r 100000 -c 20
	flag.IntVar(&recordPerCommit, "r", 1, "records number per commit")
	flag.IntVar(&commitNum, "c", 1, "commits number")
	flag.IntVar(&insertInterval, "i", 1, "insertion interval")
	flag.Parse()

	connect := createClickHouseClient()

	SetupCloseHandler()
	writeRecords(recordPerCommit, commitNum, insertInterval, connect)

}
