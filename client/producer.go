package client

import (
	"errors"
	"log"
	"net/rpc"
	"strings"
)

type Producer struct {
	serverAddress string
}

func NewProducer(serverAddress string) *Producer {
	return &Producer{
		serverAddress: serverAddress,
	}
}

func (p *Producer) CreateTopic(topicName string, noOfPartition int) error {
	topicName = strings.TrimSpace(topicName)

	if topicName == "" {
		return errors.New("invalid topic name")
	}

	noOfPartition = max(1, noOfPartition)

	return createNewTopic(p.serverAddress, topicName, noOfPartition)
}

func (p *Producer) Produce(topicName, key, value string, partitionId int) error {
	topicName = strings.TrimSpace(topicName)

	if topicName == "" {
		return errors.New("invalid topic name")
	}

	return produceNewTopic(p.serverAddress, topicName, key, value, partitionId)
}

func produceNewTopic(serverAddress, topicName, key, value string, partitionId int) error {
	client, err := rpc.Dial("tcp", serverAddress)

	if err != nil {
		return err
	}

	defer client.Close()

	var response ServerResponse

	err = client.Call(serverName, &ServerRequest{
		Action:      writeAction,
		TopicName:   topicName,
		Key:         key,
		Value:       value,
		PartitionId: partitionId,
	}, &response)

	if err != nil {
		return err
	}

	if !response.IsSuccess {
		return errors.New(response.Error)
	}

	if response.IsRedirect {
		return produceNewTopic(response.Location, topicName, key, value, partitionId)
	}

	log.Printf("Produced key = %s, value = %s \n", key, value)
	return nil
}

func createNewTopic(serverAddress, topicName string, noOfPartition int) error {
	client, err := rpc.Dial("tcp", serverAddress)

	if err != nil {
		return err
	}

	defer client.Close()

	var response ServerResponse

	err = client.Call(serverName, &ServerRequest{
		Action:        createTopic,
		TopicName:     topicName,
		NoOfPartition: noOfPartition,
	}, &response)

	if err != nil {
		return err
	}

	if !response.IsSuccess {
		return errors.New(response.Error)
	}

	if response.IsRedirect {
		return createNewTopic(response.Location, topicName, noOfPartition)
	}

	return nil
}
