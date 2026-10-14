package client

import (
	"context"
	"errors"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Options struct {
	Endpoint string
	Timeout  time.Duration
}

func (options Options) Validate() error {
	if options.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if options.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	return nil
}

func New(options Options) (*clientv3.Client, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{options.Endpoint},
		DialTimeout: options.Timeout,
	})
}

func WithDeadline(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, errors.New("parent context is required")
	}
	if timeout <= 0 {
		return nil, nil, errors.New("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}

func RawKV(client *clientv3.Client) etcdserverpb.KVClient {
	return etcdserverpb.NewKVClient(client.ActiveConnection())
}

func RawWatch(client *clientv3.Client) etcdserverpb.WatchClient {
	return etcdserverpb.NewWatchClient(client.ActiveConnection())
}
