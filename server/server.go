package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/iamvineettiwari/go-distributed-queue/constants"
	"github.com/iamvineettiwari/go-distributed-queue/coordinator"
	"github.com/iamvineettiwari/go-distributed-queue/topic"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const serverKeyPrefix = "/servers/"
const topicKeyPrefix = "/topics/"
const offsetsPrefix = "/offsets/"

var LogsFlushTime = 200 * time.Millisecond

var configPath = "config/"
var errInternalServer = errors.New("something went wrong")

const (
	internalReadAction  = "READ_INTERNAL"
	internalWriteAction = "WRITE_INTERNAL"
	internalCreateTopic = "CREATE_TOPIC_INTERNAL"
	internalAckMessage  = "ACK_MESSAGE_INTERNAL"
	internalInitClient  = "INIT_CLIENT_INTERNAL"
)

type topicConfig struct {
	Id             string `json:"id"`
	Name           string `json:"nm"`
	PartitionNo    int    `json:"pn"`
	TotalParitions int    `json:"tp"`
}

type Server struct {
	id                 string
	address            string
	PeersAddress       []string
	coordinator        *coordinator.Coordinator
	noOfMessagePerRead int
	ticker             *time.Ticker
	rootPath           string
	leaseId            clientv3.LeaseID
	topicConf          map[string]*topicConfig

	serverLock *sync.RWMutex
	topics     map[string]*topic.Topic
}

func NewServer(address string, noOfMessagePerRead int, coordinatorAddress []string) (*Server, error) {
	rootPath := fmt.Sprintf("./%s/%s", address, "data")
	configPath := filepath.Join(rootPath, "/config/")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if er := os.MkdirAll(configPath, 0777); er != nil {
			return nil, er
		}
	}

	server := &Server{
		address:            address,
		id:                 uuid.NewString(),
		topics:             make(map[string]*topic.Topic),
		topicConf:          make(map[string]*topicConfig),
		serverLock:         &sync.RWMutex{},
		noOfMessagePerRead: noOfMessagePerRead,
		coordinator:        coordinator.NewCoordinator(coordinatorAddress),
		ticker:             time.NewTicker(LogsFlushTime),
		rootPath:           rootPath,
	}

	return server, nil
}

func (s *Server) ProcessCommand(req *constants.ServerRequest, res *constants.ServerResponse) error {
	if req.TopicName == "" {
		return errors.New("invalid request, topicName is required")
	}

	switch req.Action {
	case constants.InitClient:
		return s.handleClientInitReq(req, res)
	case internalInitClient:
		return s.handleInternalClientInitReq(req, res)
	case constants.ReadAction:
		return s.handleReadReq(req, res)
	case internalReadAction:
		return s.handleInternalReadReq(req, res)
	case constants.WriteAction:
		return s.handleWriteReq(req, res)
	case internalWriteAction:
		return s.handleInternalWriteReq(req, res)
	case constants.CreateTopic:
		return s.handleCreateTopicReq(req, res)
	case internalCreateTopic:
		return s.handleInternalCreateTopicReq(req, res)
	case constants.AckMessage:
		return s.handleAck(req, res)
	case internalAckMessage:
		return s.handleInternalAck(req, res)
	default:
		res.Error = "Action not found"
		return nil
	}
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

	defer func() {
		listener.Close()
		s.Stop()
	}()

	lease, err := s.coordinator.Grant(context.Background(), 10)

	if err != nil {
		log.Fatalf("error occured while getting lease %v", err)
	}

	s.leaseId = lease.ID

	_, err = s.coordinator.Put(context.Background(), serverKeyPrefix, s.address, s.address, clientv3.WithLease(s.leaseId))

	if err != nil {
		log.Fatalf("error occured while registering self %v", err)
	}

	log.Println("Registered server with coordinator")
	go s.coordinator.KeepAlive(s.leaseId)

	if err := s.loadTopicConfigs(); err != nil {
		return err
	}

	log.Println("Topics loaded")

	go func() {
		for range s.ticker.C {
			s.flushTopicConfigs()
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

	s.ticker.Stop()
	s.coordinator.Stop()

	for _, topic := range s.topics {
		topic.CleanUp()
	}

	fmt.Println("Closed server")
}

func (s *Server) getTopicConfig(topicName string) (*topicConfig, error) {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	for _, value := range s.topicConf {
		if value.Name == topicName {
			return value, nil
		}
	}
	return nil, fmt.Errorf("topic (%s) not found", topicName)
}

func (s *Server) handleClientInitReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicConfig, err := s.getTopicConfig(req.TopicName)

	if err != nil {
		identity := s.getIdentity(req.TopicName, max(1, req.PartitionId))
		identityServer, errSrv := s.getServerForIdentity(identity)

		if errSrv != nil {
			return err
		}

		res.IsRedirect = true
		res.Location = identityServer
		return nil
	}

	data := []byte{}

	for partitionId := 1; partitionId <= topicConfig.TotalParitions; partitionId++ {
		identity := s.getIdentity(req.TopicName, partitionId)
		serverAddr, err := s.getServerForIdentity(identity)

		if err != nil {
			continue
		}

		if serverAddr != s.address {
			remoteResp := &constants.ServerResponse{}

			remoteErr := s.performInternalCall(&constants.ServerRequest{
				Action:      internalInitClient,
				TopicName:   req.TopicName,
				PartitionId: partitionId,
				ClientId:    req.ClientId,
			}, remoteResp, serverAddr)

			if remoteErr != nil {
				return remoteErr
			}
		} else {
			err := s.initClient(identity, req.TopicName, req.ClientId)
			if err != nil {
				return nil
			}
		}
	}

	res.Data = data
	res.IsSuccess = true
	return nil
}

func (s *Server) handleAck(req *constants.ServerRequest, res *constants.ServerResponse) error {
	identity := s.getIdentity(req.TopicName, req.PartitionId)
	serverAddr, err := s.getServerForIdentity(identity)

	if err != nil {
		return err
	}

	if serverAddr != s.address {
		req.Action = internalAckMessage
		return s.performInternalCall(req, res, serverAddr)
	}

	err = s.ack(identity, req.TopicName, req.ClientId, req.Offset, req.TimeStamp)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleReadReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicConfig, err := s.getTopicConfig(req.TopicName)

	if err != nil {
		identity := s.getIdentity(req.TopicName, max(1, req.PartitionId))
		identityServer, errSrv := s.getServerForIdentity(identity)

		if errSrv != nil {
			return err
		}

		res.IsRedirect = true
		res.Location = identityServer
		return nil
	}

	if req.PartitionId > 0 {

		if req.PartitionId > topicConfig.TotalParitions {
			return errors.New("partition id is invalid")
		}

		identity := s.getIdentity(req.TopicName, req.PartitionId)
		serverAddr, err := s.getServerForIdentity(identity)

		if err != nil {
			return err
		}

		if serverAddr != s.address {
			res.IsRedirect = true
			res.Location = serverAddr

			return s.performInternalCall(&constants.ServerRequest{
				Action:      internalReadAction,
				TopicName:   req.TopicName,
				PartitionId: req.PartitionId,
				ClientId:    req.ClientId,
			}, res, serverAddr)
		}

		data, err := s.read(identity, req.TopicName, req.ClientId)

		if err != nil {
			return err
		}

		res.IsSuccess = true
		res.Data = data
		return nil
	}

	return s.handleReadTopicAllPartitionReq(req, res, topicConfig)
}

func (s *Server) handleReadTopicAllPartitionReq(req *constants.ServerRequest, res *constants.ServerResponse, config *topicConfig) error {
	data := []byte{}

	for partitionId := 1; partitionId <= config.TotalParitions; partitionId++ {
		identity := s.getIdentity(req.TopicName, partitionId)

		serverAddr, err := s.getServerForIdentity(identity)

		if err != nil {
			continue
		}

		if serverAddr != s.address {
			curPartitionResp := &constants.ServerResponse{}

			err := s.performInternalCall(&constants.ServerRequest{
				Action:      internalReadAction,
				TopicName:   req.TopicName,
				PartitionId: partitionId,
				ClientId:    req.ClientId,
			}, curPartitionResp, serverAddr)

			if err != nil {
				continue
			}

			if curPartitionResp.IsSuccess && len(curPartitionResp.Data) > 0 {
				data = append(data, curPartitionResp.Data...)
			}
		} else {
			curPartitionData, err := s.read(identity, req.TopicName, req.ClientId)

			if err != nil {
				continue
			}

			data = append(data, curPartitionData...)
		}
	}

	res.IsSuccess = true
	res.Data = data
	return nil
}

func (s *Server) handleWriteReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicConfig, err := s.getTopicConfig(req.TopicName)

	if err != nil {
		identity := s.getIdentity(req.TopicName, max(1, req.PartitionId))
		identityServer, errSrv := s.getServerForIdentity(identity)

		if errSrv != nil {
			return err
		}

		res.IsRedirect = true
		res.Location = identityServer
		return nil
	}

	var serverToWrite, identity string
	var errAddr error
	var partitionId int

	if req.PartitionId > 0 {
		identity = s.getIdentity(req.TopicName, req.PartitionId)
		serverToWrite, errAddr = s.getServerForIdentity(identity)
		partitionId = req.PartitionId
	} else {
		identity, serverToWrite, partitionId, errAddr = s.GetServerToWriteData(topicConfig, req.Key, req.Value)
	}

	if errAddr != nil {
		return errAddr
	}

	if serverToWrite != s.address {
		return s.performInternalCall(&constants.ServerRequest{
			Action:      internalWriteAction,
			TopicName:   req.TopicName,
			PartitionId: partitionId,
			Key:         req.Key,
			Value:       req.Value,
		}, res, serverToWrite)
	}

	err = s.write(identity, req.TopicName, req.Key, req.Value)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleCreateTopicReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicName := req.TopicName
	noOfPartitions := max(1, req.NoOfPartition)

	for partitionId := 1; partitionId <= noOfPartitions; partitionId++ {
		serverAddr, err := s.getServerForTopicAndPartition(topicName, partitionId)

		if err != nil {
			return errInternalServer
		}

		if s.address != serverAddr {
			err := s.performInternalCall(&constants.ServerRequest{
				Action:        internalCreateTopic,
				TopicName:     topicName,
				PartitionId:   partitionId,
				NoOfPartition: noOfPartitions,
			}, &constants.ServerResponse{}, serverAddr)

			if err != nil {
				return err
			}
		} else {
			identity := s.getIdentity(topicName, partitionId)
			err = s.createTopic(identity, topicName, partitionId, noOfPartitions)

			if err != nil {
				return err
			}
		}
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleInternalClientInitReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	identity := s.getIdentity(req.TopicName, req.PartitionId)
	err := s.initClient(identity, req.TopicName, req.ClientId)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleInternalAck(req *constants.ServerRequest, res *constants.ServerResponse) error {
	identity := s.getIdentity(req.TopicName, req.PartitionId)
	err := s.ack(identity, req.TopicName, req.ClientId, req.Offset, req.TimeStamp)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleInternalCreateTopicReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicName := req.TopicName
	partitionId := req.PartitionId

	identity := s.getIdentity(topicName, partitionId)

	err := s.createTopic(identity, topicName, partitionId, req.NoOfPartition)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleInternalWriteReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicName := req.TopicName
	partitionId := req.PartitionId

	identity := s.getIdentity(topicName, partitionId)

	err := s.write(identity, topicName, req.Key, req.Value)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	return nil
}

func (s *Server) handleInternalReadReq(req *constants.ServerRequest, res *constants.ServerResponse) error {
	topicName := req.TopicName
	identity := s.getIdentity(topicName, req.PartitionId)
	data, err := s.read(identity, topicName, req.ClientId)

	if err != nil {
		return err
	}

	res.IsSuccess = true
	res.Data = data
	return nil
}

func (s *Server) performInternalCall(req *constants.ServerRequest, res *constants.ServerResponse, serverAddress string) error {
	client, err := rpc.Dial("tcp", serverAddress)

	if err != nil {
		return errInternalServer
	}

	return client.Call("Server.ProcessCommand", req, res)
}

func (s *Server) createTopic(identity, topicName string, partitionNo, totalParitions int) error {
	if topicName == "" {
		return errors.New("topicName is required")
	}

	s.serverLock.Lock()
	defer s.serverLock.Unlock()

	if _, topicExists := s.topics[identity]; topicExists {
		return fmt.Errorf("topic (%s) already exists", topicName)
	}

	config := &topicConfig{
		Id:             identity,
		Name:           topicName,
		PartitionNo:    partitionNo,
		TotalParitions: totalParitions,
	}

	topicInst, err := topic.NewTopic(s.rootPath, identity, topicName, partitionNo)

	if err != nil {
		return err
	}

	s.topics[identity] = topicInst
	s.topicConf[identity] = config
	s.saveTopicConfig()
	s.coordinator.Put(context.Background(), topicKeyPrefix, identity, s.address, clientv3.WithLease(s.leaseId))
	return nil
}

func (s *Server) write(id, topicName, key, value string) error {
	s.serverLock.Lock()
	defer s.serverLock.Unlock()

	topic, topicExists := s.topics[id]

	if !topicExists {
		return fmt.Errorf("topic (%s) does not exists", topicName)
	}

	return topic.Write(key, value)
}

func (s *Server) read(id, topicName, clientId string) ([]byte, error) {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[id]

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

	resp := []byte{}

	for _, item := range data {
		curData, err := json.Marshal(item)

		if err != nil {
			return nil, err
		}

		curData = append(curData, '\n')
		resp = append(resp, curData...)
	}

	return resp, nil
}

func (s *Server) ack(id, topicName, clientId string, offset int, timestamp time.Time) error {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[id]

	if !topicExists {
		return fmt.Errorf("topic (%s) does not exists", topicName)
	}

	return topic.Ack(clientId, offset, timestamp)
}

func (s *Server) initClient(id, topicName, clientId string) error {
	s.serverLock.RLock()
	defer s.serverLock.RUnlock()

	topic, topicExists := s.topics[id]

	if !topicExists {
		return fmt.Errorf("topic (%s) does not exists", topicName)
	}

	err := topic.InitClient(clientId)

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) loadTopicConfigs() error {
	serverTopicPath := filepath.Join(s.rootPath, configPath, "topics.db")

	if _, err := os.Stat(serverTopicPath); os.IsNotExist(err) {
		if _, er := os.OpenFile(serverTopicPath, os.O_CREATE, 0666); er != nil {
			return er
		}
	}

	localConfig, err := os.ReadFile(serverTopicPath)

	if err != nil {
		return err
	}

	s.serverLock.Lock()
	defer s.serverLock.Unlock()

	for _, item := range bytes.Split(localConfig, []byte("\n")) {
		if string(item) == "" {
			continue
		}

		config := &topicConfig{}
		err := json.Unmarshal(item, config)

		if err != nil {
			return err
		}

		topic, err := topic.NewTopic(s.rootPath, config.Id, config.Name, config.PartitionNo)

		if err != nil {
			return err
		}

		s.topicConf[config.Id] = config
		s.topics[config.Id] = topic
		s.coordinator.Put(context.Background(), topicKeyPrefix, config.Id, s.address, clientv3.WithLease(s.leaseId))
	}

	return nil
}

func (s *Server) saveTopicConfig() {
	serverTopicPath := filepath.Join(s.rootPath, configPath, "topics.db")
	file, err := os.OpenFile(serverTopicPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)

	if err != nil {
		log.Printf("Error occured while openeing file %s\n", serverTopicPath)
		return
	}

	defer file.Close()

	dataToWrite := []byte{}

	for _, conf := range s.topicConf {
		confByte, err := json.Marshal(conf)

		if err == nil {
			dataToWrite = append(dataToWrite, confByte...)
			dataToWrite = append(dataToWrite, '\n')
		}
	}

	print(string(dataToWrite))

	file.Write(dataToWrite)
}

func (s *Server) flushTopicConfigs() {
	serverTopicPath := filepath.Join(s.rootPath, configPath, "topics.db")
	file, err := os.OpenFile(serverTopicPath, os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		log.Printf("Error occured while openeing file %s\n", serverTopicPath)
		return
	}

	defer file.Close()
	file.Sync()
}

func (s *Server) GetServerToWriteData(config *topicConfig, key, value string) (string, string, int, error) {
	hash := getHash(fmt.Sprintf("%s#%s#%s", config.Name, key, value))
	partitionId := (hash % config.TotalParitions) + 1

	identity := s.getIdentity(config.Name, partitionId)
	address, err := s.getServerForIdentity(identity)

	return identity, address, partitionId, err
}

func (s *Server) getServerForIdentity(identity string) (string, error) {
	key := fmt.Sprintf("%s%s", topicKeyPrefix, identity)

	serverAddr, err := s.coordinator.Get(key)

	if err != nil || serverAddr == "" {
		log.Printf("could not get server for identity = %s", identity)
		return "", errInternalServer
	}

	return serverAddr, nil
}

func (s *Server) getServerForTopicAndPartition(topicName string, partitionId int) (string, error) {
	identity := s.getIdentity(topicName, partitionId)
	hash := getHash(identity)

	servers, err := s.coordinator.GetAll(serverKeyPrefix)

	if err != nil {
		return "", err
	}

	totalServers := len(servers)
	identityIdx := hash % totalServers

	return servers[identityIdx], nil
}

func (s *Server) getIdentity(topicName string, partitionId int) string {
	return fmt.Sprintf("%s/%d", topicName, partitionId)
}

func getHash(key string) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())
}
