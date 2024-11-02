package topic

import (
	"fmt"
	"testing"

	"github.com/iamvineettiwari/go-distributed-queue/partition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createPath(test *testing.T) {
	test.Helper()

	partition.RootPath = test.TempDir()
	partition.ConfigPath = test.TempDir()

}

func TestNewTopic(test *testing.T) {
	topicName := "test-topic"
	noOfPartitions := 3

	createPath(test)
	t, err := NewTopic(topicName, noOfPartitions)
	require.NoError(test, err, "creating topic should not produce an error")
	require.NotNil(test, t, "topic should not be nil")
	test.Cleanup(t.CleanUp)

	assert.Equal(test, noOfPartitions, t.GetNoOfPartitions(), "topic should have the correct number of partitions")
}

func TestTopic_WriteAndRead(test *testing.T) {
	topicName := "test-topic"
	noOfPartitions := 3

	createPath(test)

	t, err := NewTopic(topicName, noOfPartitions)
	require.NoError(test, err, "creating topic should not produce an error")
	require.NotNil(test, t, "topic should not be nil")

	_, err = t.Write("key1", "value1")
	require.NoError(test, err, "writing data should not produce an error")

	_, err = t.Write("key2", "value2")
	require.NoError(test, err, "writing data should not produce an error")

	data, err := t.Read("client1", 1)
	require.NoError(test, err, "reading data should not produce an error")
	assert.NotEmpty(test, data, "data read from the topic should not be empty")
	assert.GreaterOrEqual(test, len(data), 2, "there should be at least two items in the data")
	test.Cleanup(t.CleanUp)
}

func TestTopic_WriteAndReadRoundRobin(test *testing.T) {
	topicName := "test-topic"
	noOfPartitions := 2

	createPath(test)

	t, err := NewTopic(topicName, noOfPartitions)
	require.NoError(test, err, "creating topic should not produce an error")
	require.NotNil(test, t, "topic should not be nil")

	lastId := -1

	for i := 0; i < 4; i++ {
		id, err := t.Write(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
		require.NoError(test, err, "writing data should not produce an error")
		require.NotEqual(test, id, lastId, "last id should not be equal to current id")
		lastId = id
	}

	d, err := t.ReadFromPartition("client1", 1, 5)
	require.NoError(test, err, "no error expected")
	require.Equal(test, 2, len(d), "Required 2 data from current partition")

	d, err = t.ReadFromPartition("client1", 2, 15)
	require.NoError(test, err, "no error expected")
	require.Equal(test, 2, len(d), "Required 2 data from current partition")
	test.Cleanup(t.CleanUp)
}

func TestTopic_WriteToPartitionAndReadFromPartition(test *testing.T) {
	topicName := "test-topic"
	noOfPartitions := 2
	partitionID := 1

	createPath(test)

	t, err := NewTopic(topicName, noOfPartitions)
	require.NoError(test, err, "creating topic should not produce an error")
	require.NotNil(test, t, "topic should not be nil")

	err = t.WriteToPartition("key1", "value1", partitionID)
	require.NoError(test, err, "writing data to a specific partition should not produce an error")

	data, err := t.ReadFromPartition("client1", partitionID, 1)
	require.NoError(test, err, "reading data from a specific partition should not produce an error")
	require.NotEmpty(test, data, "data read from the partition should not be empty")

	assert.Equal(test, "value1", data[0].Value, "data value should match what was written")
	assert.Equal(test, "key1", data[0].Key, "data key should match what was written")
	test.Cleanup(t.CleanUp)
}

func TestTopic_ReadFromInvalidPartition(test *testing.T) {
	topicName := "test-topic"
	noOfPartitions := 2

	createPath(test)

	t, err := NewTopic(topicName, noOfPartitions)
	require.NoError(test, err, "creating topic should not produce an error")
	require.NotNil(test, t, "topic should not be nil")

	_, err = t.ReadFromPartition("client1", 99, 5)
	assert.Error(test, err, "reading from an invalid partition should produce an error")
	test.Cleanup(t.CleanUp)
}

func TestTopic_WriteToInvalidPartition(test *testing.T) {
	topicName := "test-topic"
	noOfPartitions := 2
	createPath(test)

	t, err := NewTopic(topicName, noOfPartitions)
	require.NoError(test, err, "creating topic should not produce an error")
	require.NotNil(test, t, "topic should not be nil")

	err = t.WriteToPartition("key1", "value1", 99)
	assert.Error(test, err, "writing to an invalid partition should produce an error")
	test.Cleanup(t.CleanUp)
}
