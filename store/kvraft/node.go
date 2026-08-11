package kvraft

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/fredouric/kvraft/store"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

var _ store.Store = (*Node)(nil)

const (
	retainSnapshotCount = 3
	raftTimeout         = 10 * time.Second
)

type Node struct {
	raft       *raft.Raft
	innerStore store.Store

	transport     *raft.NetworkTransport
	snapshotStore raft.SnapshotStore
	boltDB        *raftboltdb.BoltStore

	id   raft.ServerID
	addr raft.ServerAddress
}

func NewNode(store store.Store, localID string, bindAddr string, raftDir string) (*Node, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, err
	}
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, err
	}

	fsm := NewFSM(store)

	r, err := raft.NewRaft(config, fsm, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return nil, err
	}

	return &Node{raft: r, id: raft.ServerID(localID), addr: transport.LocalAddr(), transport: transport, boltDB: boltDB, snapshotStore: snapshots, innerStore: store}, nil
}

func (n *Node) Bootstrap() error {
	hasState, err := raft.HasExistingState(n.boltDB, n.boltDB, n.snapshotStore)
	if err != nil {
		return err
	}
	if hasState {
		return nil
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      n.id,
				Address: n.addr,
			},
		},
	}
	return n.raft.BootstrapCluster(configuration).Error()
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		if n.raft.Leader() != "" {
			return nil
		}
		select {
		case <-ticker.C:
		case <-deadline:
			return fmt.Errorf("timed out after %s waiting for a leader", timeout)
		}
	}
}

func (n *Node) Close() error {
	if err := n.raft.Shutdown().Error(); err != nil {
		return err
	}
	if err := n.transport.Close(); err != nil {
		return err
	}
	return n.boltDB.Close()
}

func (n *Node) Get(key string) (string, bool, error) {
	return n.innerStore.Get(key)
}

func (n *Node) Set(key string, value string) error {
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	cmd := Command{Op: OpSet, Key: key, Value: value}
	b, err := Encode(cmd)
	if err != nil {
		return err
	}

	return n.raft.Apply(b, raftTimeout).Error()
}

func (n *Node) Delete(key string) error {
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	cmd := Command{Op: OpDelete, Key: key}
	b, err := Encode(cmd)
	if err != nil {
		return err
	}

	return n.raft.Apply(b, raftTimeout).Error()
}

func (n *Node) Join(nodeID, addr string) error {

	f := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := f.Error(); err != nil {
		return err
	}
	slog.Info("node joined successfully", "nodeID", nodeID, "addr", addr)
	return nil
}
