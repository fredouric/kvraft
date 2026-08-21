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

	n1, err := NewNode(memory.New(), "node1", addr, dir, 256)
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

	n2, err := NewNode(memory.New(), "node1", addr, dir, 256)
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

// TestSnapshotRestore proves the snapshot path. It writes data, forces a
// snapshot (which compacts the Raft log), then restarts the node with a fresh
// empty in-memory store. The data can only reappear through FSM.Restore,
// because the log entries that carried it were compacted away.
func TestSnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	addr := freePort(t)

	n1, err := NewNode(memory.New(), "node1", addr, dir, 256)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := n1.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := n1.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Write KV data and advertise an address, so BOTH pieces of FSM state
	// (the store and the nodes map) must survive the snapshot.
	if err := n1.Set("color", "blue"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := n1.Advertise("node1", "127.0.0.1:9999"); err != nil {
		t.Fatalf("Advertise: %v", err)
	}

	// Force a snapshot. This calls FSM.Snapshot + Persist and compacts the log.
	if err := n1.group.Snapshot().Error(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := n1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restart with a BRAND-NEW empty store. Only Restore can refill it.
	n2, err := NewNode(memory.New(), "node1", addr, dir, 256)
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

	// KV data must be back.
	eventually(t, 5*time.Second, func() bool {
		v, ok, _ := n2.Get("color")
		return ok && v == "blue"
	}, "KV data did not survive snapshot restore")

	// The nodes map must be back too (the debt we closed).
	if addr, ok := n2.fsm.AddrFor("node1"); !ok || addr != "127.0.0.1:9999" {
		t.Fatalf("AddrFor(node1) = %q, %v; want 127.0.0.1:9999, true", addr, ok)
	}
}
