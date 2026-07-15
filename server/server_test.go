package server_test

import (
	"net/rpc"
	"testing"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/server"
	"github.com/fredouric/kvraft/store/memory"
)

func newTestClient(t *testing.T) *rpc.Client {
	t.Helper()

	kv := &server.KVService{Store: memory.New()}
	s := server.New(":0", kv)
	if err := s.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}

	client, err := rpc.DialHTTP("tcp", s.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { client.Close() })

	return client
}

func TestSetGetRoundTrip(t *testing.T) {
	// Arrange
	client := newTestClient(t)

	// Act
	setArgs := &kvapi.SetArgs{Key: "foo", Value: "bar"}
	if err := client.Call("KVService.Set", setArgs, &kvapi.Empty{}); err != nil {
		t.Fatalf("set: %v", err)
	}

	var reply kvapi.GetReply
	if err := client.Call("KVService.Get", &kvapi.GetArgs{Key: "foo"}, &reply); err != nil {
		t.Fatalf("get: %v", err)
	}

	// Assert
	if reply.Value != "bar" {
		t.Errorf("Get(foo) = %q, want %q", reply.Value, "bar")
	}
	if !reply.Found {
		t.Errorf("Get(foo) = %q, want Found=True", reply.Value)
	}
}

func TestSetDeleteRoundTrip(t *testing.T) {
	// Arrange
	client := newTestClient(t)

	// Act
	setArgs := &kvapi.SetArgs{Key: "foo", Value: "bar"}
	if err := client.Call("KVService.Set", setArgs, &kvapi.Empty{}); err != nil {
		t.Fatalf("set: %v", err)
	}

	var reply kvapi.Empty
	if err := client.Call("KVService.Delete", &kvapi.DeleteArgs{Key: "foo"}, &reply); err != nil {
		t.Fatalf("get: %v", err)
	}

	var getReply kvapi.GetReply
	if err := client.Call("KVService.Get", &kvapi.GetArgs{Key: "foo"}, &getReply); err != nil {
		t.Fatalf("get: %v", err)
	}
	// Assert
	if getReply.Found {
		t.Errorf("want Found=False")
	}

}
