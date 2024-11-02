package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/iamvineettiwari/go-distributed-queue/client"
)

func subscribeToTopic(data []byte) {
	log.Println(string(data))
}

var isConsumer = flag.Bool("isConsumer", true, "Want to run consumer ?")
var topic = flag.String("topic", "", "Topic name to consume / produce to")

func main() {
	flag.Parse()

	topicName := strings.TrimSpace(*topic)

	if topicName == "" {
		log.Fatal("topic name is required")
	}

	if *isConsumer {
		consumer := client.NewConsumer("127.0.0.1:8080")
		consumer.Subscribe(topicName, -1, subscribeToTopic)

		select {}
	} else {
		producer := client.NewProducer("127.0.0.1:8080")
		producer.CreateTopic(topicName, 3)

		for {
			var key, value string

			fmt.Print("Enter key: ")
			_, err := fmt.Scanln(&key)
			if err != nil {
				log.Println("Error occurred while reading key from stdin:", err)
				continue
			}

			fmt.Print("Enter value: ")
			_, err = fmt.Scanln(&value)
			if err != nil {
				log.Println("Error occurred while reading value from stdin:", err)
				continue
			}

			err = producer.Produce(topicName, key, value, -1)
			if err != nil {
				log.Println("Error occurred while producing message:", err)
			} else {
				log.Printf("Produced message to topic %s with key: %s, value: %s\n", topicName, key, value)
			}
		}
	}
}
