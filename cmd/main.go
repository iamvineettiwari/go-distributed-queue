package main

import (
	"flag"
	"log"
	"strings"

	"github.com/iamvineettiwari/go-distributed-queue/server"
)

var serverType = flag.String("serverType", "", "type of server - server / coordinator")
var address = flag.String("addr", "", "address <host:port> for current server")
var coordinatorAdress = flag.String("cAddr", "", "address <host:port> for coordinator, seperated by comma `,`")
var nMessagePerRead = flag.Int("mpr", 1, "no of message per read")

func parseServerType() {
	flag.Parse()

	if *serverType == "" {
		log.Fatal("serverType is required")
	}

	if *serverType != "server" && *serverType != "coordinator" {
		log.Fatal("serverType must be server / coordinator")
	}
}

func startBroker() {
	if strings.TrimSpace(*address) == "" {
		log.Fatal("addr is required")
	}

	crdAddr := strings.Split(*coordinatorAdress, ",")

	processedCAdrr := []string{}

	for _, item := range crdAddr {
		item = strings.TrimSpace(item)

		if item != "" {
			processedCAdrr = append(processedCAdrr, item)
		}
	}

	if len(processedCAdrr) == 0 {
		log.Fatal("cAddr is required")
	}

	server := server.NewServer(strings.TrimSpace(*address), *nMessagePerRead, processedCAdrr)
	server.Start()
}

func startCoordinator() {

}

func main() {
	parseServerType()

	if *serverType == "server" {
		startBroker()
		return
	}

	startCoordinator()
}
