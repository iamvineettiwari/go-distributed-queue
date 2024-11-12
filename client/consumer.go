package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"time"

	"github.com/google/uuid"
	"github.com/iamvineettiwari/go-distributed-queue/constants"
)

type SubscribeCallback func(data *Data)

type Consumer struct {
	id            string
	pollTime      time.Duration
	serverAddress string
	topicName     string
}

func NewConsumer(consumerId, serverAddress, topicName string, pollTime time.Duration) *Consumer {
	if consumerId == "" {
		consumerId = uuid.NewString()
	}

	if pollTime <= 0 {
		pollTime = 100 * time.Millisecond
	}

	consumer := &Consumer{
		id:            consumerId,
		serverAddress: serverAddress,
		pollTime:      pollTime,
		topicName:     topicName,
	}

	err := consumer.init()

	if err != nil {
		log.Fatal(err)
	}

	return consumer
}

func (c *Consumer) SetPollTime(duration time.Duration) {
	if duration < 0 {
		return
	}

	c.pollTime = duration
}

func (c *Consumer) Subscribe(partitionId int, fn SubscribeCallback) {
	go func() {
		for {
			c.handleSubscribe(partitionId, fn)
			time.Sleep(c.pollTime)
		}
	}()
}

func (c *Consumer) handleSubscribe(partitionId int, fn SubscribeCallback) {
	resp, err := c.call(&constants.ServerRequest{
		Action:      constants.ReadAction,
		PartitionId: partitionId,
		TopicName:   c.topicName,
		ClientId:    c.id,
	})

	if err == nil {
		if resp.Error == "" && len(resp.Data) > 0 {
			for _, item := range bytes.Split(resp.Data, []byte("\n")) {
				if string(item) == "" {
					continue
				}

				data := &Data{}

				err := json.Unmarshal(item, data)

				if err != nil {
					log.Println(err)
					continue
				}

				data.ClientId = c.id
				data.ServerAddr = c.serverAddress

				fn(data)
			}
		}

		if resp.IsRedirect {
			c.serverAddress = resp.Location
		}
	}
}

func (c *Consumer) init() error {
	resp, err := c.call(&constants.ServerRequest{
		Action:    constants.InitClient,
		TopicName: c.topicName,
		ClientId:  c.id,
	})

	if err != nil {
		return err
	}

	if resp.IsRedirect {
		c.serverAddress = resp.Location
		return c.init()
	}
	return nil
}

func (c *Consumer) call(serverRequest *constants.ServerRequest) (*constants.ServerResponse, error) {
	client, err := rpc.Dial("tcp", c.serverAddress)

	if err != nil {
		client.Close()
		log.Fatalf("Unable to connect to server %s\n", c.serverAddress)
	}
	defer client.Close()

	response := &constants.ServerResponse{}

	err = client.Call(constants.ServerName, serverRequest, response)
	return response, err
}

func getIdentity(topicName string, partitionId int) string {
	return fmt.Sprintf("%s/%d", topicName, partitionId)
}
