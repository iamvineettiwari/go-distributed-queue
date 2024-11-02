package partition

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func createPath(t *testing.T) {
	t.Helper()

	RootPath = t.TempDir()
	ConfigPath = t.TempDir()
}

func TestNewPartition(t *testing.T) {
	createPath(t)

	partition, err := NewPartition(1, "test_topic", "test_store")
	if err != nil {
		t.Fatalf("Failed to create new partition: %v", err)
	}

	if partition.GetId() != 1 {
		t.Errorf("Expected partition ID 1, got %d", partition.GetId())
	}

	if partition.topicName != "test_topic" {
		t.Errorf("Expected topic name 'test_topic', got '%s'", partition.topicName)
	}

	if partition.logStorePath != filepath.Join(RootPath, "test_store") {
		t.Errorf("Expected log store path '%s', got '%s'", filepath.Join(RootPath, "test_store"), partition.logStorePath)
	}

	// Clean up
	partition.CleanUp()
}

func TestWriteData(t *testing.T) {
	createPath(t)

	partition, err := NewPartition(1, "test_topic", "test_store")
	if err != nil {
		t.Fatalf("Failed to create new partition: %v", err)
	}
	defer partition.CleanUp()

	key := "test_key"
	value := "test_value"
	err = partition.WriteData(key, value)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	data, err := partition.ReadData("client_1", 1)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(data) != 1 {
		t.Errorf("Expected 1 record, got %d", len(data))
	}

	expectedData := Data{
		Key:       key,
		Value:     value,
		Partition: partition.id,
		Topic:     partition.topicName,
	}

	if data[0].Key != expectedData.Key || data[0].Value != expectedData.Value {
		t.Errorf("Expected data %v, got %v", expectedData, data[0])
	}
}

func TestReadData(t *testing.T) {
	createPath(t)

	partition, err := NewPartition(1, "test_topic", "test_store")
	if err != nil {
		t.Fatalf("Failed to create new partition: %v", err)
	}
	defer partition.CleanUp()

	for i := 0; i < 5; i++ {
		err = partition.WriteData(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	data, err := partition.ReadData("client_1", 3)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(data) != 3 {
		t.Errorf("Expected to read 3 records, got %d", len(data))
	}
}

func TestPersistSizeIndexes(t *testing.T) {
	createPath(t)

	partition, err := NewPartition(1, "test_topic", "test_store")
	if err != nil {
		t.Fatalf("Failed to create new partition: %v", err)
	}
	defer partition.CleanUp()

	err = partition.WriteData("key", "value")
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	err = partition.persistSizeIndexes()
	if err != nil {
		t.Fatalf("Failed to persist size indexes: %v", err)
	}

	var persistedIndex map[int]*SizeLog
	configData, err := os.ReadFile(partition.configLogPath)
	if err != nil {
		t.Fatalf("Failed to read config log: %v", err)
	}

	if err := json.Unmarshal(configData, &persistedIndex); err != nil {
		t.Fatalf("Failed to unmarshal persisted size indexes: %v", err)
	}

	if len(persistedIndex) == 0 {
		t.Error("Expected size indexes to be persisted, but found none")
	}
}
