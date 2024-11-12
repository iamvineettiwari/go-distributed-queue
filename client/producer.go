package client

import (
	"errors"
	"net/rpc"
	"strings"

	"github.com/iamvineettiwari/go-distributed-queue/constants"
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

	var response constants.ServerResponse

	err = client.Call(constants.ServerName, &constants.ServerRequest{
		Action:      constants.WriteAction,
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
	return nil
}

func createNewTopic(serverAddress, topicName string, noOfPartition int) error {
	client, err := rpc.Dial("tcp", serverAddress)

	if err != nil {
		return err
	}

	defer client.Close()

	var response constants.ServerResponse

	err = client.Call(constants.ServerName, &constants.ServerRequest{
		Action:        constants.CreateTopic,
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
