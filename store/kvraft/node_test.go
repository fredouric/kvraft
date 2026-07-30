package kvraft

import (
	"net"
	"testing"
	"time"

	"github.com/fredouric/kvraft/store/memory"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestSingleNodeWriteReadAndReplay(t *testing.T) {
	dir := t.TempDir()
	addr := freePort(t)

	n1, err := NewNode(memory.New(), "node1", addr, dir)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := n1.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := n1.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	if err := n1.Set("foo", "bar"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, ok, _ := n1.Get("foo"); !ok || v != "bar" {
		t.Fatalf("Get after Set = (%q, %v), want (\"bar\", true)", v, ok)
	}

	if err := n1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	n2, err := NewNode(memory.New(), "node1", addr, dir)
	if err != nil {
		t.Fatalf("NewNode (restart): %v", err)
	}
	if err := n2.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap (restart): %v", err)
	}
	if err := n2.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader (restart): %v", err)
	}
	defer n2.Close()

	eventually(t, 5*time.Second, func() bool {
		v, ok, _ := n2.Get("foo")
		return ok && v == "bar"
	}, "value did not survive restart — Raft log was not replayed through FSM.Apply")
}
