package topic

import (
	"time"

	"github.com/google/uuid"
	"github.com/iamvineettiwari/go-distributed-queue/topic/partition"
)

type Topic struct {
	id          string
	name        string
	rootPath    string
	partitionNo int

	partition *partition.Partition
}

func NewTopic(rootPath string, id string, name string, partitionNo int) (*Topic, error) {
	if id == "" {
		id = uuid.NewString()
	}

	topic := &Topic{
		id:          id,
		name:        name,
		rootPath:    rootPath,
		partitionNo: partitionNo,
	}

	newPartition, err := partition.NewPartition(rootPath, partitionNo, name)

	if err != nil {
		return nil, err
	}

	topic.partition = newPartition
	return topic, nil
}

func (t *Topic) CleanUp() {
	t.partition.CleanUp()
}

func (t *Topic) GetId() string {
	return t.id
}

func (t *Topic) GetName() string {
	return t.name
}

func (t *Topic) GetPartition() *partition.Partition {
	return t.partition
}

func (t *Topic) Read(clientId string, numRecords int) (data []partition.Data, err error) {
	return t.partition.ReadData(clientId, numRecords)
}

func (t *Topic) Write(key, value string) error {
	return t.partition.WriteData(key, value)
}

func (t *Topic) InitClient(clientId string) error {
	return t.partition.InitClient(clientId)
}

func (t *Topic) Ack(clientId string, offset int, timestamp time.Time) error {
	return t.partition.Ack(clientId, offset, timestamp)
}
