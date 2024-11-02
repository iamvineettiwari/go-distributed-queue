package coordinator

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Coordinator struct {
	client *clientv3.Client
}

func NewCoordinator(client *clientv3.Client) *Coordinator {
	return &Coordinator{
		client: client,
	}
}

func (c *Coordinator) WaitForBrokers(minBrokers int) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		resp, err := c.client.Get(ctx, "/brokers/", clientv3.WithPrefix())

		if err != nil {
			return nil, err
		}

		brokers := []string{}

		for _, kv := range resp.Kvs {
			brokers = append(brokers, string(kv.Value))
		}

		if len(brokers) >= minBrokers {
			return brokers, nil
		}

		time.Sleep(2 * time.Second)
	}
}
