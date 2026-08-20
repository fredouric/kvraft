package raftgroup

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const retainSnapshotCount = 3

type Group struct {
	raft          *raft.Raft
	transport     *raft.NetworkTransport
	snapshotStore raft.SnapshotStore
	boltDB        *raftboltdb.BoltStore

	id   raft.ServerID
	addr raft.ServerAddress
}

func New(fsm raft.FSM, localID, bindAddr, raftDir string) (*Group, error) {
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

	r, err := raft.NewRaft(config, fsm, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return nil, err
	}

	return &Group{
		raft:          r,
		transport:     transport,
		snapshotStore: snapshots,
		boltDB:        boltDB,
		id:            raft.ServerID(localID),
		addr:          transport.LocalAddr(),
	}, nil
}

func (g *Group) Bootstrap() error {
	hasState, err := raft.HasExistingState(g.boltDB, g.boltDB, g.snapshotStore)
	if err != nil {
		return err
	}
	if hasState {
		return nil
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{{ID: g.id, Address: g.addr}},
	}
	return g.raft.BootstrapCluster(configuration).Error()
}

func (g *Group) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		if g.raft.Leader() != "" {
			return nil
		}
		select {
		case <-ticker.C:
		case <-deadline:
			return fmt.Errorf("timed out after %s waiting for a leader", timeout)
		}
	}
}

func (g *Group) Close() error {
	if err := g.raft.Shutdown().Error(); err != nil {
		return err
	}
	if err := g.transport.Close(); err != nil {
		return err
	}
	return g.boltDB.Close()
}

func (g *Group) Apply(b []byte, timeout time.Duration) (interface{}, error) {
	f := g.raft.Apply(b, timeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Response(), nil
}

func (g *Group) IsLeader() bool {
	return g.raft.State() == raft.Leader
}

func (g *Group) AddVoter(id, addr string) error {
	return g.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0).Error()
}

func (g *Group) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	return g.raft.LeaderWithID()
}

func (g *Group) Snapshot() raft.Future {
	return g.raft.Snapshot()
}
