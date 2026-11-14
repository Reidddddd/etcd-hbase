package scenario

import (
	"context"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
)

// Run starts a private embedded etcd node, executes the pinned native suite,
// and writes lossless request/response observations as JSONL.
func Run(ctx context.Context, suite Suite, dataDir string, output io.Writer) (Metadata, error) {
	if ctx == nil {
		return Metadata{}, errors.New("scenario context is required")
	}
	if err := validateSuite(suite); err != nil {
		return Metadata{}, fmt.Errorf("native scenario was not loaded from the pinned manifest: %w", err)
	}
	if output == nil {
		return Metadata{}, errors.New("trace output writer is required")
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return Metadata{}, fmt.Errorf("create isolated etcd data directory: %w", err)
	}
	server, err := startEmbedded(ctx, dataDir)
	if err != nil {
		return Metadata{}, err
	}
	defer server.close()

	metadata, err := BuildMetadata(suite)
	if err != nil {
		return Metadata{}, err
	}
	runner := &runner{client: server.client, writer: output, ordinals: make(map[string]uint64), evidenceScope: "native-etcd"}
	if err := runner.run(ctx); err != nil {
		return metadata, err
	}
	return metadata, nil
}

type embedded struct {
	server *embed.Etcd
	client *clientv3.Client
}

func startEmbedded(parent context.Context, dataDir string) (*embedded, error) {
	return startEmbeddedWithElectionTimeout(parent, dataDir, 0)
}

func startEmbeddedWithElectionTimeout(parent context.Context, dataDir string,
	electionTimeoutMs uint) (*embedded, error) {
	peerURL, err := loopbackURL()
	if err != nil {
		return nil, fmt.Errorf("allocate peer listener: %w", err)
	}
	clientURL, err := loopbackURL()
	if err != nil {
		return nil, fmt.Errorf("allocate client listener: %w", err)
	}
	cfg := embed.NewConfig()
	cfg.Name = "m0-native"
	cfg.Dir = filepath.Join(dataDir, "data")
	if electionTimeoutMs != 0 {
		cfg.ElectionMs = electionTimeoutMs
	}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"stderr"}
	server, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("start embedded etcd: %w", err)
	}
	startCtx, cancel := context.WithTimeout(parent, readyTimeout)
	defer cancel()
	select {
	case <-server.Server.ReadyNotify():
	case err := <-server.Err():
		server.Close()
		return nil, fmt.Errorf("embedded etcd exited before ready: %w", err)
	case <-startCtx.Done():
		server.Server.Stop()
		server.Close()
		return nil, fmt.Errorf("embedded etcd ready timeout: %w", startCtx.Err())
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{clientURL.Host}, DialTimeout: requestTimeout, Logger: zap.NewNop(),
	})
	if err != nil {
		server.Close()
		return nil, fmt.Errorf("connect to embedded etcd: %w", err)
	}
	return &embedded{server: server, client: client}, nil
}

func (server *embedded) close() {
	if server.client != nil {
		_ = server.client.Close()
	}
	if server.server != nil {
		server.server.Close()
	}
}

func loopbackURL() (*url.URL, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return nil, err
	}
	return &url.URL{Scheme: "http", Host: address}, nil
}
