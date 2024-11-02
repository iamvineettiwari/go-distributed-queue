package client

import (
	"errors"
	"net/rpc"
	"time"

	"github.com/google/uuid"
)

type SubscribeCallback func(data []byte)

type Consumer struct {
	id            string
	pollTime      time.Duration
	brokerAddress string
}

func NewConsumer(brokerAddress string) *Consumer {
	return &Consumer{
		id:            uuid.NewString(),
		brokerAddress: brokerAddress,
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
			data, err := c.poll(topic, partitionId)

			if err != nil && len(data) > 0 {
				fn(data)
			}

			time.Sleep(c.pollTime)
		}
	}()
}

func (c *Consumer) poll(topic string, partitionId int) ([]byte, error) {
	client, err := rpc.Dial("tcp", c.brokerAddress)

	if err != nil {
		return nil, err
	}
	defer client.Close()

	var response ServerResponse

	client.Call(serverName, &ServerRequest{
		Action:      readAction,
		PartitionId: partitionId,
		TopicName:   topic,
		ClientId:    c.id,
	}, &response)

	return response.Data, errors.New(response.Error)
}
