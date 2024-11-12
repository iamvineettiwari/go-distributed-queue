package server_test

import (
	"context"
	"net/rpc"
	"os"
	"testing"
	"time"

	"github.com/iamvineettiwari/go-distributed-queue/constants"
	"github.com/iamvineettiwari/go-distributed-queue/server"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type MockCoordinator struct{}

func (m *MockCoordinator) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1}, nil
}

func (m *MockCoordinator) Put(ctx context.Context, prefix, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

func (m *MockCoordinator) KeepAlive(id clientv3.LeaseID) {}

func (m *MockCoordinator) Get(key string) (string, error) {
	return "127.0.0.1:12345", nil
}

func (m *MockCoordinator) Stop() {}

func TestServerProcessCommand(t *testing.T) {
	srv, err := server.NewServer("127.0.0.1:12345", 10, []string{"localhost:2379"})
	assert.NoError(t, err)

	go func() {
		err := srv.Start()
		if err != nil {
			t.Errorf("Server start error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	client, err := rpc.Dial("tcp", "127.0.0.1:12345")
	assert.NoError(t, err)
	defer client.Close()

	createReq := &constants.ServerRequest{
		Action:        "CREATE_TOPIC",
		TopicName:     "TestTopic",
		NoOfPartition: 1,
	}
	createRes := &constants.ServerResponse{}

	err = client.Call("Server.ProcessCommand", createReq, createRes)
	assert.NoError(t, err)
	assert.True(t, createRes.IsSuccess)

	writeReq := &constants.ServerRequest{
		Action:      "WRITE",
		TopicName:   "TestTopic",
		PartitionId: 1,
		Key:         "test-key",
		Value:       "test-value",
	}
	writeRes := &constants.ServerResponse{}

	err = client.Call("Server.ProcessCommand", writeReq, writeRes)
	assert.NoError(t, err)
	assert.True(t, writeRes.IsSuccess)

	readReq := &constants.ServerRequest{
		Action:      "READ",
		TopicName:   "TestTopic",
		PartitionId: 1,
	}
	readRes := &constants.ServerResponse{}

	err = client.Call("Server.ProcessCommand", readReq, readRes)
	assert.NoError(t, err)
	assert.True(t, readRes.IsSuccess)
	assert.Contains(t, string(readRes.Data), "test-value")

	srv.Stop()
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
