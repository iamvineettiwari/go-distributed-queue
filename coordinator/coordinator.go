package coordinator

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Coordinator struct {
	client *clientv3.Client
}

func NewCoordinator(address []string) *Coordinator {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("Error occured %v \n", err)
	}

	return &Coordinator{
		client: client,
	}
}

func (c *Coordinator) Stop() {
	c.client.Close()
}

func (c *Coordinator) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return c.client.Grant(ctx, ttl)
}

func (c *Coordinator) KeepAlive(leaseId clientv3.LeaseID) {
	ch, err := c.client.KeepAlive(context.Background(), leaseId)

	if err != nil {
		log.Printf("Error occured while keepAlive loop %v\n", err)
	}

	for range ch {
	}
}

func (c *Coordinator) Put(ctx context.Context, keyPrefix, key, value string, options ...clientv3.OpOption) (bool, error) {
	keyPath := fmt.Sprintf("%s%s", keyPrefix, key)
	_, err := c.client.Put(ctx, keyPath, value, options...)
	return err != nil, err
}

func (c *Coordinator) Get(key string) (string, error) {
	resp, err := c.client.Get(context.Background(), key)

	if err != nil {
		return "", err
	}

	if len(resp.Kvs) > 0 {
		return string(resp.Kvs[0].Value), nil
	}

	return "", fmt.Errorf("key (%s) does not exists", key)
}

func (c *Coordinator) GetAll(keyPrefix string) ([]string, error) {
	resp, err := c.client.Get(context.Background(), keyPrefix, clientv3.WithPrefix())

	if err != nil {
		return nil, err
	}

	data := []string{}

	for _, item := range resp.Kvs {
		data = append(data, string(item.Value))
	}

	return data, nil
}
