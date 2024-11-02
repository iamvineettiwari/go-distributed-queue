package partition

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var RootPath = "./static/data/"
var ConfigPath = "./static/config/"
var LogsFlushTime = 200 * time.Millisecond

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

	partitionLock *sync.RWMutex
	configLogPath string
	logStorePath  string
	logOffset     int
	lastLogOffset int
	logSizeIndex  map[int]*SizeLog
	clientOffset  map[string]*ClientOffset
}

func NewPartition(id int, topicName, storePathName string) (*Partition, error) {
	if _, err := os.Stat(ConfigPath); os.IsNotExist(err) {
		if er := os.MkdirAll(ConfigPath, 0777); er != nil {
			return nil, er
		}
	}

	if _, err := os.Stat(RootPath); os.IsNotExist(err) {
		if er := os.MkdirAll(RootPath, 0777); er != nil {
			return nil, er
		}
	}

	partition := &Partition{
		id:            id,
		topicName:     topicName,
		logStorePath:  filepath.Join(RootPath, storePathName),
		configLogPath: filepath.Join(ConfigPath, fmt.Sprintf("%s_%d_size_idx.db", topicName, id)),
		partitionLock: &sync.RWMutex{},
		logSizeIndex:  make(map[int]*SizeLog),
		clientOffset:  make(map[string]*ClientOffset),
		ticker:        time.NewTicker(LogsFlushTime),
	}

	if err := partition.loadSizeIndexes(); err != nil {
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

	offset, present := p.clientOffset[clientId]

	if !present {
		offset = &ClientOffset{
			CurrentOffset: 0,
		}
	}

	d, err := p.readData(offset.CurrentOffset, numRecords)

	if err != nil && err != io.EOF {
		return nil, err
	}

	offset.CurrentOffset += len(d)
	p.clientOffset[clientId] = offset

	if err == io.EOF {
		return nil, err
	}

	return d, err
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

func (p *Partition) flushLogs() {
	p.partitionLock.RLock()
	defer p.partitionLock.RUnlock()

	if p.lastLogOffset < p.logOffset {
		file, err := os.OpenFile(p.logStorePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

		if err == nil {
			if err = file.Sync(); err == nil {
				p.lastLogOffset = p.logOffset
			}
		}
	}

	file, err := os.OpenFile(p.configLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

	if err == nil {
		file.Sync()
	}
}
