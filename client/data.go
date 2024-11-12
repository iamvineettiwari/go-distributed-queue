package client

import (
	"log"
	"time"

	"github.com/iamvineettiwari/go-distributed-queue/constants"
)

type Data struct {
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	Timestamp  time.Time `json:"timestamp"`
	Offset     int       `json:"offset"`
	Partition  int       `json:"partition"`
	Topic      string    `json:"topic"`
	ClientId   string    `json:"client_id"`
	ServerAddr string    `json:"srv_addr"`
}

func (d *Data) Ack() {
	consumer := NewConsumer(d.ClientId, d.ServerAddr, d.Topic, 0)
	newOffset := d.Offset + 1

	_, err := consumer.call(&constants.ServerRequest{
		Action:      constants.AckMessage,
		ClientId:    consumer.id,
		TopicName:   d.Topic,
		PartitionId: d.Partition,
		TimeStamp:   time.Now(),
		Offset:      newOffset,
	})

	if err != nil {
		log.Println(err)
	}
}
