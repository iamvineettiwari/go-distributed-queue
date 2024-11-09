package client

import (
	"log"
	"net/rpc"
	"time"

	"github.com/google/uuid"
)

type SubscribeCallback func(data []byte)

type Consumer struct {
	id            string
	pollTime      time.Duration
	serverAddress string
}

func NewConsumer(serverAddress string) *Consumer {
	return &Consumer{
		id:            uuid.NewString(),
		serverAddress: serverAddress,
		pollTime:      10 * time.Millisecond,
	}
}

func (c *Consumer) SetPollTime(duration time.Duration) {
	if duration < 0 {
		return
	}

	c.pollTime = duration
}

func (c *Consumer) Subscribe(topic string, partitionId int, fn SubscribeCallback) {
	go func() {
		for {
			resp, err := c.poll(topic, partitionId)

			if err == nil {
				if resp.Error == "" && len(resp.Data) > 0 {
					fn(resp.Data)
				}

				if resp.IsRedirect {
					c.serverAddress = resp.Location
				}
			} else {
				log.Println(err)
			}

			time.Sleep(c.pollTime)
		}
	}()
}

func (c *Consumer) poll(topic string, partitionId int) (*ServerResponse, error) {
	client, err := rpc.Dial("tcp", c.serverAddress)

	if err != nil {
		client.Close()
		log.Fatalf("Unable to connect to topic %s\n", topic)
	}
	defer client.Close()

	var response ServerResponse

	err = client.Call(serverName, &ServerRequest{
		Action:      readAction,
		PartitionId: partitionId,
		TopicName:   topic,
		ClientId:    c.id,
	}, &response)

	return &response, err
}
