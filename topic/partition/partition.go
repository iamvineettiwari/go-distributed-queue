package partition

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var LogsFlushTime = 5 * time.Minute

type Data struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Offset    int       `json:"offset"`
	Partition int       `json:"partition"`
	Topic     string    `json:"topic"`
}

type SizeLog struct {
	Size   int `json:"size"`
	Offset int `json:"offset"`
}

type LastCommitedOffset struct {
	Offset    int       `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

type ClientOffset struct {
	CurrentOffset      int                 `json:"current_offset"`
	LastCommitedOffset *LastCommitedOffset `json:"last_commited_offset"`
}

type Partition struct {
	id        int
	topicName string
	ticker    *time.Ticker

	partitionLock   *sync.RWMutex
	configLogPath   string
	logStorePath    string
	offsetStorePath string
	logOffset       int
	lastLogOffset   int
	logSizeIndex    map[int]*SizeLog
	clientOffset    map[string]*ClientOffset
}

func NewPartition(rootPath string, id int, topicName string) (*Partition, error) {
	configPath := filepath.Join(rootPath, topicName, "/configs/")
	dbPath := filepath.Join(rootPath, topicName, "/db/")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if er := os.MkdirAll(configPath, 0777); er != nil {
			return nil, er
		}
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if er := os.MkdirAll(dbPath, 0777); er != nil {
			return nil, er
		}
	}

	partition := &Partition{
		id:              id,
		topicName:       topicName,
		logStorePath:    filepath.Join(dbPath, fmt.Sprintf("%s_%d_store.db", topicName, id)),
		configLogPath:   filepath.Join(configPath, fmt.Sprintf("%s_%d_size_idx.db", topicName, id)),
		offsetStorePath: filepath.Join(configPath, fmt.Sprintf("%s_%d_client_offset.db", topicName, id)),
		partitionLock:   &sync.RWMutex{},
		logSizeIndex:    make(map[int]*SizeLog),
		clientOffset:    make(map[string]*ClientOffset),
		ticker:          time.NewTicker(LogsFlushTime),
	}

	if err := partition.load(); err != nil {
		return nil, err
	}

	partition.logOffset = len(partition.logSizeIndex)

	go func() {
		for range partition.ticker.C {
			partition.flushLogs()
		}
	}()

	return partition, nil
}

func (p *Partition) GetId() int {
	return p.id
}

func (p *Partition) WriteData(key string, value string) error {
	p.partitionLock.Lock()
	defer p.partitionLock.Unlock()

	file, err := os.OpenFile(p.logStorePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

	if err != nil {
		return err
	}

	defer file.Close()

	data := Data{
		Key:       key,
		Value:     value,
		Offset:    p.logOffset,
		Timestamp: time.Now(),
		Partition: p.id,
		Topic:     p.topicName,
	}

	marshalledData, err := json.Marshal(data)

	if err != nil {
		return err
	}

	marshalledData = append(marshalledData, '\n')
	curOffset := 0

	if lastOffset, ok := p.logSizeIndex[p.logOffset-1]; ok {
		curOffset += lastOffset.Size + lastOffset.Offset
	}

	p.logSizeIndex[p.logOffset] = &SizeLog{
		Size:   len(marshalledData),
		Offset: curOffset,
	}
	p.logOffset++

	if err := p.persistSizeIndexes(); err != nil {
		return err
	}

	_, err = file.Write(marshalledData)

	return err
}

func (p *Partition) ReadData(clientId string, numRecords int) ([]Data, error) {
	p.partitionLock.Lock()
	defer p.partitionLock.Unlock()

	clientOffset, present := p.clientOffset[clientId]

	if !present {
		clientOffset = &ClientOffset{
			CurrentOffset: 0,
		}
	}

	d, err := p.readData(clientOffset.CurrentOffset, numRecords)

	if err != nil && err != io.EOF {
		return nil, err
	}

	clientOffset.CurrentOffset += len(d)
	p.clientOffset[clientId] = clientOffset

	if er := p.persistClientOffsets(); er != nil {
		return nil, err
	}

	if err == io.EOF {
		return nil, err
	}

	return d, err
}

func (p *Partition) Ack(clientId string, newOffset int, timestamp time.Time) error {
	p.partitionLock.Lock()
	defer p.partitionLock.Unlock()

	offset, exists := p.clientOffset[clientId]

	if !exists {
		return errors.New("invalid request")
	}

	if offset.LastCommitedOffset != nil && offset.LastCommitedOffset.Offset >= newOffset {
		return nil
	}

	offset.CurrentOffset = max(offset.CurrentOffset, newOffset)
	offset.LastCommitedOffset = &LastCommitedOffset{
		Timestamp: timestamp,
		Offset:    newOffset,
	}
	p.clientOffset[clientId] = offset
	p.persistClientOffsets()
	return nil
}

func (p *Partition) InitClient(clientId string) error {
	p.partitionLock.Lock()
	defer p.partitionLock.Unlock()

	offset, exists := p.clientOffset[clientId]

	if !exists {
		offset = &ClientOffset{
			CurrentOffset: 0,
		}
	}

	if offset.LastCommitedOffset == nil {
		offset.CurrentOffset = 0
	} else {
		offset.CurrentOffset = offset.LastCommitedOffset.Offset
	}

	p.clientOffset[clientId] = offset
	err := p.persistClientOffsets()

	if err != nil {
		return err
	}

	return nil
}

func (p *Partition) CleanUp() {
	p.ticker.Stop()
}

func (p *Partition) readData(offset, numRecords int) (data []Data, err error) {
	numRecords = max(numRecords, 1) - 1
	lastOffset := min(offset+numRecords, len(p.logSizeIndex)-1)

	curLogSize, present := p.logSizeIndex[offset]

	if !present {
		return data, io.EOF
	}

	totalSize := 0

	for i := offset; i <= lastOffset; i++ {
		if curSize, present := p.logSizeIndex[i]; present {
			totalSize += curSize.Size
		}
	}

	file, err := os.Open(p.logStorePath)

	if err != nil {
		return data, err
	}

	defer file.Close()

	if _, err := file.Seek(int64(curLogSize.Offset), 0); err != nil {
		return data, err
	}

	buf := make([]byte, totalSize)

	n, err := file.Read(buf)

	if err != nil {
		return data, err
	}

	recData := buf[:n]

	for _, item := range bytes.Split(recData, []byte("\n")) {
		if string(item) == "" {
			continue
		}

		var curData Data
		err = json.Unmarshal(item, &curData)
		data = append(data, curData)
	}

	return data, err
}

func (p *Partition) load() error {
	if err := p.loadSizeIndexes(); err != nil {
		return err
	}

	if err := p.loadClientOffsets(); err != nil {
		return err
	}

	return nil
}

func (p *Partition) loadClientOffsets() error {
	if _, err := os.Stat(p.offsetStorePath); os.IsNotExist(err) {
		if _, er := os.OpenFile(p.offsetStorePath, os.O_CREATE, 0666); er != nil {
			return er
		}
	}

	configData, err := os.ReadFile(p.offsetStorePath)

	if err != nil {
		return err
	}

	if len(configData) == 0 {
		return nil
	}

	return json.Unmarshal(configData, &p.clientOffset)
}

func (p *Partition) loadSizeIndexes() error {
	if _, err := os.Stat(p.configLogPath); os.IsNotExist(err) {
		if _, er := os.OpenFile(p.configLogPath, os.O_CREATE, 0666); er != nil {
			return er
		}
	}

	configData, err := os.ReadFile(p.configLogPath)

	if err != nil {
		return err
	}

	if len(configData) == 0 {
		return nil
	}

	return json.Unmarshal(configData, &p.logSizeIndex)
}

func (p *Partition) persistSizeIndexes() error {
	file, err := os.OpenFile(p.configLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)

	if err != nil {
		return err
	}

	defer file.Close()

	configData, err := json.Marshal(p.logSizeIndex)

	if err != nil {
		return err
	}
	_, err = file.Write(configData)

	if err != nil {
		return err
	}

	return nil
}

func (p *Partition) persistClientOffsets() error {
	file, err := os.OpenFile(p.offsetStorePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)

	if err != nil {
		return err
	}

	defer file.Close()

	configData, err := json.Marshal(p.clientOffset)

	if err != nil {
		return err
	}
	_, err = file.Write(configData)

	if err != nil {
		return err
	}

	return nil
}

func (p *Partition) flushLogs() {
	p.partitionLock.RLock()
	defer p.partitionLock.RUnlock()

	go flush(p.configLogPath)
	go flush(p.offsetStorePath)

	if p.lastLogOffset < p.logOffset {
		err := flush(p.logStorePath)
		if err == nil {
			p.lastLogOffset = p.logOffset

		}
	}
}

func flush(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		return err
	}
	defer file.Close()

	return file.Sync()
}
