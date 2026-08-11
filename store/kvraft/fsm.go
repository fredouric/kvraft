package kvraft

import (
	"encoding/gob"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/fredouric/kvraft/store"
	"github.com/hashicorp/raft"
)

type FSM struct {
	s store.Store

	mu    sync.RWMutex
	nodes map[string]string
}

func NewFSM(s store.Store) *FSM {
	return &FSM{s: s, nodes: make(map[string]string)}
}

func (f *FSM) AddrFor(id string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	addr, ok := f.nodes[id]
	return addr, ok
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	cmd, err := Decode(log.Data)
	if err != nil {
		panic(fmt.Errorf("unable to decode data: %s", err))
	}
	switch cmd.Op {
	case OpSet:
		if err := f.s.Set(cmd.Key, cmd.Value); err != nil {
			panic(fmt.Errorf("unable to apply set: %s", err))
		}
	case OpDelete:
		if err := f.s.Delete(cmd.Key); err != nil {
			panic(fmt.Errorf("unable to apply delete: %s", err))
		}
	case OpAddNode:
		f.mu.Lock()
		f.nodes[cmd.Key] = cmd.Value
		f.mu.Unlock()
	default:
		panic(fmt.Errorf("unrecognized command: %s", cmd.Op))

	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	snap, ok := f.s.(store.Snapshotter)
	if !ok {
		return nil, fmt.Errorf("unable to snapshot store")
	}

	dump, err := snap.Dump()
	if err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	nodesCopy := make(map[string]string)
	maps.Copy(nodesCopy, f.nodes)

	return &Snapshot{KV: dump, Nodes: nodesCopy}, nil

}
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	snapshot := &Snapshot{}
	if err := gob.NewDecoder(rc).Decode(snapshot); err != nil {
		return err
	}
	snap, ok := f.s.(store.Snapshotter)
	if !ok {
		return fmt.Errorf("unable to restore store")
	}
	if err := snap.Restore(snapshot.KV); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes = make(map[string]string, len(snapshot.Nodes))
	maps.Copy(f.nodes, snapshot.Nodes)
	return nil
}

type Snapshot struct {
	KV    map[string]string
	Nodes map[string]string
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if err := gob.NewEncoder(sink).Encode(s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {}
