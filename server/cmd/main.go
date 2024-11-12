package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/iamvineettiwari/go-distributed-queue/server"
)

var address = flag.String("addr", "", "address <host:port> for current server")
var coordinatorAdress = flag.String("cAddr", "", "address <host:port> for coordinator, seperated by comma `,`")
var nMessagePerRead = flag.Int("mpr", 1, "no of message per read")

func getServer() *server.Server {
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

	server, err := server.NewServer(strings.TrimSpace(*address), *nMessagePerRead, processedCAdrr)

	if err != nil {
		log.Fatal(err)
	}

	return server
}

func main() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	flag.Parse()
	server := getServer()

	go func() {
		for range quit {
			server.Stop()
			os.Exit(0)
		}
	}()

	server.Start()
}
