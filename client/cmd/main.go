package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/iamvineettiwari/go-distributed-queue/client"
)

var mode = flag.String("mode", "", "which mode you want to run ? producer / consumer")

func main() {
	flag.Parse()

	switch *mode {
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	default:
		log.Fatalf("invalid mode - %s\n", *mode)
	}
}

func runProducer() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Enter bootstrap server address: ")
	bootstrapServerAddr := readNext(scanner)

	if bootstrapServerAddr == "" {
		log.Fatal("invalid bootstrap server addr")
	}

	producer := client.NewProducer(bootstrapServerAddr)

	for {

		fmt.Print("Enter mode:\n1. Produce\n2. Create Topic\n")
		modeOpt := readNext(scanner)

		switch modeOpt {
		case "1":
			produce(scanner, producer)
		case "2":
			createNewTopic(scanner, producer)
		default:
			log.Println("invalid input")
		}
	}
}

func runConsumer() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Enter bootstrap server address: ")
	bootstrapServerAddr := readNext(scanner)

	if bootstrapServerAddr == "" {
		log.Fatal("invalid bootstrap server addr")
	}

	fmt.Print("Enter client id (leave empty to generate new): ")
	clientId := readNext(scanner)

	fmt.Print("Enter poll time (in ms): ")
	pollTimeStr := readNext(scanner)

	pollTime, err := strconv.Atoi(pollTimeStr)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("Enter topic: ")
	topic := readNext(scanner)

	consumer := client.NewConsumer(clientId, bootstrapServerAddr, topic, time.Duration(pollTime)*time.Millisecond)

	fmt.Print("Enter partitionId (0 - to read from all the paritions, greater that 0 to read from specific partition): ")
	partition := readNext(scanner)

	partitionId, err := strconv.Atoi(partition)

	if err != nil {
		log.Println("errror occured ", err)
		return
	}

	consumer.Subscribe(partitionId, func(data *client.Data) {
		log.Printf("DATA - %v\n", data)
		data.Ack()
	})

	select {}
}

func produce(scanner *bufio.Scanner, producer *client.Producer) {
	fmt.Print("Enter topic: ")
	topic := readNext(scanner)

	fmt.Print("Enter key: ")
	key := readNext(scanner)

	fmt.Print("Enter value: ")
	value := readNext(scanner)

	fmt.Print("Enter partitionId (0 - to produce in random partition, greater that 0 to produce to specific partition): ")
	partition := readNext(scanner)

	partitionId, err := strconv.Atoi(partition)

	if err != nil {
		log.Println("errror occured ", err)
		return
	}

	err = producer.Produce(topic, key, value, partitionId)

	if err != nil {
		log.Println(err)
	} else {
		log.Println("successfully produced message")
	}
}

func createNewTopic(scanner *bufio.Scanner, producer *client.Producer) {
	fmt.Print("Enter topic: ")
	topic := readNext(scanner)

	fmt.Print("Enter partitions: ")
	partitions := readNext(scanner)

	totalPartitions, err := strconv.Atoi(partitions)

	if err != nil {
		log.Println("errror occured ", err)
		return
	}

	err = producer.CreateTopic(topic, totalPartitions)

	if err != nil {
		log.Println(err)
	}
}

func readNext(scanner *bufio.Scanner) string {
	scanner.Scan()

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error occured while reading from STDIN : %v\n", err)
	}
	return scanner.Text()
}
