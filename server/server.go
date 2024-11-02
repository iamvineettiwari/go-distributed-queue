package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/google/uuid"
	"github.com/iamvineettiwari/go-distributed-queue/coordinator"
	"github.com/iamvineettiwari/go-distributed-queue/topic"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var keyPrefix = "server"
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

type Server struct {
	id                 string
	address            string
	PeersAddress       []string
	etdc               *clientv3.Client
	noOfMessagePerRead int

	serverLock *sync.RWMutex
	topics     map[string]*topic.Topic
}

func NewServer(address string, noOfMessagePerRead int, coordinatorAddress []string) *Server {
	return &Server{
		address:            address,
		id:                 uuid.NewString(),
		topics:             make(map[string]*topic.Topic),
		serverLock:         &sync.RWMutex{},
		noOfMessagePerRead: noOfMessagePerRead,
		etdc:               coordinator.NewEtcdClient(coordinatorAddress),
	}
}

func (s *Server) ProcessCommand(req *ServerRequest, res *ServerResponse) error {
	if req.Action == readAction {
		var data []byte
		var err error

		if req.PartitionId > 0 {
			data, err = s.readFromPartition(req.TopicName, req.ClientId, req.PartitionId)
		} else {
			data, err = s.read(req.TopicName, req.ClientId)
		}

		if err != nil {
			res.Error = err.Error()
		}

		res.IsSuccess = err == nil
		res.Data = data
		return nil
	}

	if req.Action == writeAction {
		var created bool
		var err error

		if req.PartitionId > 0 {
			created, err = s.writeToPartition(req.TopicName, req.Key, req.Value, req.PartitionId)
		} else {
			created, err = s.write(req.TopicName, req.Key, req.Value)
		}

		if err != nil {
			res.Error = err.Error()
		}

		res.IsSuccess = created
		return nil
	}

	if req.Action == createTopic {
		created, err := s.createTopic(req.TopicName, req.NoOfPartition)
		if err != nil {
			res.Error = err.Error()
		}
		res.IsSuccess = created
		return nil
	}

	res.Error = "Action not found"
	return nil
}

func (s *Server) Start() error {
	err := rpc.Register(s)

	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.address)

	if err != nil {
		return err
	}

	log.Printf("Server [%s] is listening on address %s\n", s.id, s.address)

	defer listener.Close()

	key := fmt.Sprintf("/%s/%s", keyPrefix, s.id)
	lease, err := s.etdc.Grant(context.Background(), 10)

	if err != nil {
		log.Fatalf("error occured while getting lease %v", err)
	}

	log.Printf("Got lease from coordinate server - %v \n", lease.ID)

	_, err = s.etdc.Put(context.Background(), key, s.address, clientv3.WithLease(lease.ID))

	if err != nil {
		log.Fatalf("error occured while registering self %v", err)
	}

	log.Println("Registered server with coordinator")

	go func() {
		ch, err := s.etdc.KeepAlive(context.Background(), lease.ID)

		if err != nil {
			log.Fatalf("error occured while keeping alive %v", err)
		}

		log.Println("keepAlive loop active")

		for range ch {
		}
	}()

	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Printf("conn err occured: %v\n", err)
			continue
		}

		go rpc.ServeConn(conn)
	}
}

func (s *Server) Stop() {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	for _, topic := range s.topics {
		topic.CleanUp()
	}
}

func (s *Server) createTopic(topicName string, noOfPartition int) (bool, error) {
	if topicName == "" {
		return false, errors.New("topicName is required")
	}

	s.serverLock.Lock()
	defer s.serverLock.Unlock()

	if noOfPartition <= 0 {
		noOfPartition = 1
	}

	if _, topicExists := s.topics[topicName]; topicExists {
		return false, fmt.Errorf("topic (%s) already exists", topicName)
	}

	topic, err := topic.NewTopic(topicName, noOfPartition)

	if err != nil {
		return false, err
	}

	s.topics[topicName] = topic
	return true, nil
}

func (s *Server) write(topicName, key, value string) (bool, error) {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[topicName]

	if !topicExists {
		return false, fmt.Errorf("topic (%s) does not exists", topicName)
	}

	_, err := topic.Write(key, value)

	return err == nil, err
}

func (s *Server) writeToPartition(topicName, key, value string, partitionId int) (bool, error) {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[topicName]

	if !topicExists {
		return false, fmt.Errorf("topic (%s) does not exists", topicName)
	}

	err := topic.WriteToPartition(key, value, partitionId)
	return err == nil, err
}

func (s *Server) read(topicName, clientId string) ([]byte, error) {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[topicName]

	if !topicExists {
		return nil, fmt.Errorf("topic (%s) does not exists", topicName)
	}

	data, err := topic.Read(clientId, s.noOfMessagePerRead)

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	resp, err := json.Marshal(data)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) readFromPartition(topicName, clientId string, partitionId int) ([]byte, error) {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[topicName]

	if !topicExists {
		return nil, fmt.Errorf("topic (%s) does not exists", topicName)
	}

	data, err := topic.ReadFromPartition(clientId, partitionId, s.noOfMessagePerRead)

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	resp, err := json.Marshal(data)

	if err != nil {
		return nil, err
	}

	return resp, nil
}
