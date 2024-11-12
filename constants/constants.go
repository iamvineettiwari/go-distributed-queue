package constants

import (
	"errors"
	"time"
)

const (
	ServerName = "Server.ProcessCommand"

	ReadAction   = "READ"
	WriteAction  = "WRITE"
	CreateTopic  = "CREATE_TOPIC"
	AckMessage   = "ACK_MESSAGE"
	InitClient   = "INIT_CLIENT"
	SyncMetaData = "SYNC_METADATA"
)

var (
	ErrInternalServer  = errors.New("internal server error")
	ErrClientOffsetErr = errors.New("client offset error")
)

type ServerRequest struct {
	Action         string
	TopicName      string
	NoOfPartition  int
	Key            string
	Value          string
	PartitionId    int
	ClientId       string
	Offset         int
	TimeStamp      time.Time
	ClientMetaData map[string]int
}

type ServerResponse struct {
	Error      string
	IsSuccess  bool
	Data       []byte
	IsRedirect bool
	Location   string
}
