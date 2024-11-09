package topic

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTopic(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	id := ""
	name := "test-topic"
	partitionNo := 1

	topic, err := NewTopic(rootPath, id, name, partitionNo)
	assert.NoError(t, err, "Expected no error when creating a new topic")
	assert.NotNil(t, topic, "Expected topic to be created successfully")
	assert.Equal(t, name, topic.GetName(), "Expected topic name to match")
	assert.NotEmpty(t, topic.GetId(), "Expected topic ID to be generated if not provided")
	assert.NotNil(t, topic.GetPartition(), "Expected topic partition to be initialized")
}

func TestTopicWriteAndRead(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	name := "test-topic"
	partitionNo := 1

	topic, err := NewTopic(rootPath, "", name, partitionNo)
	assert.NoError(t, err, "Expected no error when creating a new topic")
	defer topic.CleanUp()

	key := "key1"
	value := "value1"
	err = topic.Write(key, value)
	assert.NoError(t, err, "Expected no error when writing data")

	clientID := "client1"
	numRecords := 1
	data, err := topic.Read(clientID, numRecords)
	assert.NoError(t, err, "Expected no error when reading data")
	assert.Len(t, data, 1, "Expected to read one record")

	assert.Equal(t, key, data[0].Key, "Expected key to match")
	assert.Equal(t, value, data[0].Value, "Expected value to match")
}

func TestTopicGetIdAndName(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	name := "test-topic"
	partitionNo := 1
	id := "test-id"

	topic, err := NewTopic(rootPath, id, name, partitionNo)
	assert.NoError(t, err, "Expected no error when creating a new topic")

	assert.Equal(t, id, topic.GetId(), "Expected topic ID to match the provided ID")
	assert.Equal(t, name, topic.GetName(), "Expected topic name to match the provided name")
}

func TestTopicCleanUp(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	name := "test-topic"
	partitionNo := 1

	topic, err := NewTopic(rootPath, "", name, partitionNo)
	assert.NoError(t, err, "Expected no error when creating a new topic")

	topic.CleanUp()
}

func createTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}
