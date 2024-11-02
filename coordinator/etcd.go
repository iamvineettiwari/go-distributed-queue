package coordinator

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewEtcdClient(endpoints []string) *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatal("unable to connect to coordinate server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Get(ctx, "health-check-key")

	if err != nil {
		client.Close()
		log.Fatal("unable to connect to coordinate server")
	}

	return client
}
