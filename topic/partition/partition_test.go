package partition

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func TestNewPartition(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	id := 1
	topicName := "test-topic"

	partition, err := NewPartition(rootPath, id, topicName)
	assert.NoError(t, err)
	assert.NotNil(t, partition)
	assert.Equal(t, id, partition.GetId())
	assert.Equal(t, topicName, partition.topicName)
}

func TestPartitionWriteAndReadData(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	id := 1
	topicName := "test-topic"

	partition, err := NewPartition(rootPath, id, topicName)
	assert.NoError(t, err)
	defer partition.CleanUp()

	key := "key1"
	value := "value1"

	err = partition.WriteData(key, value)
	assert.NoError(t, err)

	clientID := "client1"
	numRecords := 1
	data, err := partition.ReadData(clientID, numRecords)
	assert.NoError(t, err)
	assert.Len(t, data, 1)
	assert.Equal(t, key, data[0].Key)
	assert.Equal(t, value, data[0].Value)
}

func TestPartitionClientOffset(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	id := 1
	topicName := "test-topic"

	partition, err := NewPartition(rootPath, id, topicName)
	assert.NoError(t, err)
	defer partition.CleanUp()

	clientID := "client1"
	numRecords := 2

	for i := 1; i <= 3; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err = partition.WriteData(key, value)
		assert.NoError(t, err)
	}

	data, err := partition.ReadData(clientID, numRecords)
	assert.NoError(t, err)
	assert.Len(t, data, 2)

	_, present := partition.clientOffset[clientID]
	assert.True(t, present)
	assert.Equal(t, numRecords, partition.clientOffset[clientID].CurrentOffset)
}

func TestPartitionCleanUp(t *testing.T) {
	rootPath := createTempDir(t)
	defer os.RemoveAll(rootPath)

	id := 1
	topicName := "test-topic"

	partition, err := NewPartition(rootPath, id, topicName)
	assert.NoError(t, err)

	partition.CleanUp()

	time.Sleep(LogsFlushTime)

	select {
	case <-partition.ticker.C:
		t.Error("Expected ticker to be stopped after cleanup")
	default:

	}
}
