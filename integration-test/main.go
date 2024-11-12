package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"math/rand"

	"github.com/google/uuid"
	"github.com/iamvineettiwari/go-distributed-queue/client"
)

var (
	producerCount = flag.Int("producers", 1, "Number of producer goroutines")
	consumerCount = flag.Int("consumers", 1, "Number of consumer goroutines")
	messageCount  = flag.Int("messages", 100, "Number of messages per producer")
	clientID      = "testClient"
)

func main() {
	flag.Parse()

	fmt.Println("Testing distributed queue with multiple producers and consumers")
	bootstrapServerAddr := "localhost:8081"
	topic := uuid.NewString()
	nPartitions := 3

	producer := client.NewProducer(bootstrapServerAddr)
	if err := producer.CreateTopic(topic, nPartitions); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("Created topic %s with 3 partitions\n", topic)

	var wg sync.WaitGroup

	res := []*client.Data{}

	wg.Add(*producerCount)
	for i := 0; i < *producerCount; i++ {
		go func(producerID int) {
			defer wg.Done()
			produceMessages(producerID, bootstrapServerAddr, topic)
		}(i)
	}

	wg.Wait()

	wg.Add(*consumerCount)
	curParition := 0
	for i := 0; i < *consumerCount; i++ {
		go func(partitionId int) {
			defer wg.Done()

			res = append(res, consumeMessages(partitionId, bootstrapServerAddr, topic)...)
		}(curParition + 1)

		curParition = (curParition + 1) % nPartitions
	}

	wg.Wait()
	fmt.Println("Checking for duplicate messages...")
	checkDuplicates(res)
}

func produceMessages(producerID int, bootstrapServerAddr, topic string) {
	producer := client.NewProducer(bootstrapServerAddr)
	for i := 0; i < *messageCount; i++ {
		key := fmt.Sprintf("producer-%d-key-%d-%d", producerID, i, time.Now().UnixMilli())
		value := fmt.Sprintf("message from producer-%d, msg-%d", producerID, i)

		err := producer.Produce(topic, key, value, 0)
		if err != nil {
			log.Printf("Producer %d: failed to produce message %d - %v", producerID, i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Printf("Producer %d: finished producing %d messages", producerID, *messageCount)
}

func consumeMessages(partitionId int, bootstrapServerAddr, topic string) []*client.Data {
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(191) + 10
	consumer := client.NewConsumer(clientID, bootstrapServerAddr, topic, time.Duration(randomNumber)*time.Millisecond)

	lastMessage := time.Now()

	res := []*client.Data{}

	consumer.Subscribe(partitionId, func(data *client.Data) {
		lastMessage = time.Now()
		res = append(res, data)
		data.Ack()
	})

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if time.Since(lastMessage) > (time.Duration(randomNumber+100) * time.Millisecond) {
			break
		}
	}
	log.Println("Consumer finished.")
	return res
}

func checkDuplicates(res []*client.Data) {
	data := make(map[string]string)
	duplicates := 0

	for _, message := range res {
		key := fmt.Sprintf("%s/%d", message.Key, message.Partition)

		if existing, present := data[key]; present {
			log.Println("Duplicate ", key, existing, message.Value)
			duplicates++
		} else {
			data[key] = message.Value
		}
	}
	fmt.Printf("Total messages - %d\nUnique messages - %d\n", len(data), len(data)-duplicates)
}
