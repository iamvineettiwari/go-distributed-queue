package client

var serverName = "Server.ProcessCommand"

var readAction = "READ"
var writeAction = "WRITE"
var createTopic = "CREATE_TOPIC"

type ServerRequest struct {
	Action        string
	TopicName     string
	NoOfPartition int
	Key           string
	Value         string
	PartitionId   int
	ClientId      string
}

type ServerResponse struct {
	Error     string
	IsSuccess bool
	Data      []byte
}
