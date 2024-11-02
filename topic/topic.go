package topic

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/iamvineettiwari/go-distributed-queue/partition"
)

type Topic struct {
	id            string
	name          string
	noOfPartition int

	topicLock    *sync.RWMutex
	curPartition int
	partitions   map[int]*partition.Partition
}

func NewTopic(name string, noOfPartition int) (*Topic, error) {
	id, err := uuid.NewUUID()

	if err != nil {
		return nil, err
	}

	topic := &Topic{
		id:            id.String(),
		name:          name,
		noOfPartition: noOfPartition,
		topicLock:     &sync.RWMutex{},
	}

	noOfPartition = max(1, noOfPartition)

	partitions := make(map[int]*partition.Partition)

	for i := 1; i <= noOfPartition; i++ {
		partition, err := partition.NewPartition(i, name, fmt.Sprintf("%s_%d.db", name, i))
		if err != nil {
			return nil, err
		}

		partitions[i-1] = partition
	}

	topic.partitions = partitions
	return topic, nil
}

func (t *Topic) CleanUp() {
	for _, item := range t.partitions {
		item.CleanUp()
	}
}

func (t *Topic) GetNoOfPartitions() int {
	t.topicLock.RLock()
	defer t.topicLock.RUnlock()

	return t.noOfPartition
}

func (t *Topic) Read(clientId string, numRecords int) (data []partition.Data, err error) {
	t.topicLock.RLock()
	defer t.topicLock.RUnlock()

	wg := &sync.WaitGroup{}
	dataCh := make(chan []partition.Data, len(t.partitions))
	errCh := make(chan error, len(t.partitions))

	for _, p := range t.partitions {
		wg.Add(1)
		go t.read(p, clientId, numRecords, dataCh, errCh, wg)
	}

	go func() {
		wg.Wait()
		close(dataCh)
		close(errCh)
	}()

	if len(errCh) > 0 {
		return nil, <-errCh
	}

	for item := range dataCh {
		data = append(data, item...)
	}

	return data, nil
}

func (t *Topic) ReadFromPartition(clientId string, partitionId int, numRecords int) (data []partition.Data, err error) {
	t.topicLock.RLock()
	defer t.topicLock.RUnlock()

	p, isPresent := t.partitions[partitionId-1]

	if !isPresent {
		return nil, errors.New("partitionId is invalid")
	}

	data, err = p.ReadData(clientId, numRecords)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (t *Topic) Write(key, value string) (int, error) {
	t.topicLock.Lock()
	defer t.topicLock.Unlock()

	t.curPartition = (t.curPartition + 1) % t.noOfPartition

	if p, present := t.partitions[t.curPartition]; present {
		return p.GetId(), p.WriteData(key, value)
	}

	log.Println(t.curPartition)

	return -1, errors.New("something went wrong")
}

func (t *Topic) WriteToPartition(key, value string, partitionNo int) error {
	t.topicLock.RLock()
	defer t.topicLock.RUnlock()

	partition, present := t.partitions[partitionNo-1]

	if !present {
		return errors.New("invalid partition no")
	}

	return partition.WriteData(key, value)
}

func (t *Topic) read(p *partition.Partition, clientId string, numRecords int, dataCh chan<- []partition.Data, errCh chan<- error, wg *sync.WaitGroup) {
	curData, err := p.ReadData(clientId, numRecords)
	defer wg.Done()

	if err != nil {
		errCh <- err
		return
	}

	dataCh <- curData
}
