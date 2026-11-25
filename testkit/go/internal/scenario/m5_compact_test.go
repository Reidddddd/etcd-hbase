package scenario

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This is a native oracle, not evidence that the Java Compact RPC is assembled.
func TestM5CompactPinnedNativeBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	native, err := startEmbedded(ctx, filepath.Join(t.TempDir(), "native"))
	if err != nil {
		t.Fatal(err)
	}
	defer native.close()
	kv := etcdserverpb.NewKVClient(native.client.ActiveConnection())
	for _, test := range []struct {
		revision  int64
		compacted bool
	}{{-2, true}, {-1, true}, {0, false}, {0, true}, {-1, true}} {
		response, failure := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: test.revision})
		if test.compacted {
			requireMvccError(t, failure, rpctypes.ErrGRPCCompacted)
			if response != nil {
				t.Fatalf("rejected initial compact %d returned a response", test.revision)
			}
		} else if failure != nil || response.GetHeader().GetRevision() != 1 {
			t.Fatalf("initial compact %d: %v %v", test.revision, response, failure)
		}
	}
	key := []byte("m5/compact/boundary")
	for index := 0; index < 4; index++ {
		if _, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte{byte(index)}}); err != nil {
			t.Fatal(err)
		}
	}
	compact, err := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: 4, Physical: true})
	if err != nil || compact.GetHeader().GetRevision() != 5 {
		t.Fatalf("compact: %v %v", compact, err)
	}
	for _, test := range []struct {
		revision int64
		expected error
	}{{3, rpctypes.ErrGRPCCompacted}, {4, rpctypes.ErrGRPCCompacted}, {6, rpctypes.ErrGRPCFutureRev}, {0, rpctypes.ErrGRPCCompacted}, {-1, rpctypes.ErrGRPCCompacted}} {
		_, err := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: test.revision})
		requireMvccError(t, err, test.expected)
	}
	for _, test := range []struct {
		revision int64
		code     codes.Code
		value    byte
	}{{3, codes.OutOfRange, 0}, {4, codes.OK, 2}, {5, codes.OK, 3}, {0, codes.OK, 3}, {-1, codes.OK, 3}} {
		response, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: test.revision})
		if status.Code(err) != test.code {
			t.Fatalf("range %d: %v", test.revision, err)
		}
		if test.code == codes.OK {
			if response.GetHeader().GetRevision() != 5 || len(response.Kvs) != 1 || len(response.Kvs[0].Value) != 1 || response.Kvs[0].Value[0] != test.value {
				t.Fatalf("range %d returned unexpected boundary data: %v", test.revision, response)
			}
		} else {
			requireMvccError(t, err, rpctypes.ErrGRPCCompacted)
		}
	}
	for _, revision := range []int64{3, 4, 5, 0, -1} {
		t.Run("watch/"+fmt.Sprint(revision), func(t *testing.T) {
			watchCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			defer stop()
			stream, err := etcdserverpb.NewWatchClient(native.client.ActiveConnection()).Watch(watchCtx)
			if err != nil {
				t.Fatal(err)
			}
			err = stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, StartRevision: revision}}})
			if err != nil {
				t.Fatal(err)
			}
			created, err := stream.Recv()
			if err != nil || !created.GetCreated() || created.GetCanceled() {
				t.Fatalf("created: %v %v", created, err)
			}
			if revision == 0 {
				if _, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte{4}}); err != nil {
					t.Fatal(err)
				}
			}
			next, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if revision < 4 && revision != 0 {
				if !next.Canceled || next.CompactRevision != 4 || len(next.Events) != 0 {
					t.Fatalf("compacted: %v", next)
				}
			} else {
				expected := revision
				if revision == 0 {
					expected = 6
				}
				if next.Canceled || len(next.Events) == 0 || next.Events[0].Kv.ModRevision != expected {
					t.Fatalf("events at %d: %v", revision, next)
				}
			}
		})
	}
}

func requireMvccError(t *testing.T, actual, expected error) {
	t.Helper()
	if status.Code(actual) != status.Code(expected) || status.Convert(actual).Message() != status.Convert(expected).Message() {
		t.Fatalf("error identity: got %v; want %v", actual, expected)
	}
}
