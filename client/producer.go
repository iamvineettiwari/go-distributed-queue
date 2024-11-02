package client

import (
	"errors"
	"log"
	"net/rpc"
	"strings"
)

type Producer struct {
	brokerAddress string
}

func NewProducer(brokerAddress string) *Producer {
	return &Producer{
		brokerAddress: brokerAddress,
	}
}

func (p *Producer) CreateTopic(topicName string, noOfPartition int) error {
	topicName = strings.TrimSpace(topicName)

	if topicName == "" {
		return errors.New("invalid topic name")
	}

	noOfPartition = max(1, noOfPartition)

	client, err := rpc.Dial("tcp", p.brokerAddress)

	if err != nil {
		return err
	}

	defer client.Close()

	var response ServerResponse

	client.Call(serverName, &ServerRequest{
		Action:        createTopic,
		TopicName:     topicName,
		NoOfPartition: noOfPartition,
	}, &response)

	if !response.IsSuccess {
		return errors.New(response.Error)
	}

	return nil
}

func (p *Producer) Produce(topicName, key, value string, partitionId int) error {
	topicName = strings.TrimSpace(topicName)

	if topicName == "" {
		return errors.New("invalid topic name")
	}

	client, err := rpc.Dial("tcp", p.brokerAddress)

	if err != nil {
		return err
	}

	defer client.Close()

	var response ServerResponse

	client.Call(serverName, &ServerRequest{
		Action:      writeAction,
		TopicName:   topicName,
		Key:         key,
		Value:       value,
		PartitionId: partitionId,
	}, &response)

	log.Println(response)

	if !response.IsSuccess {
		return errors.New(response.Error)
	}
	log.Printf("Produced key = %s, value = %s \n", key, value)
	return nil
}
