package kvraft

import (
	"encoding/gob"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/fredouric/kvraft/shard"
	"github.com/fredouric/kvraft/store"
	"github.com/hashicorp/raft"
)

type FSM struct {
	s       store.Store
	nShards int

	mu        sync.RWMutex
	nodes     map[string]string
	configNum int
	served    map[int]bool
	pending   map[int]bool
}

func NewFSM(s store.Store, nShards int) *FSM {
	return &FSM{
		s:       s,
		nShards: nShards,
		nodes:   make(map[string]string),
		served:  make(map[int]bool),
		pending: make(map[int]bool),
	}
}

func (f *FSM) AddrFor(id string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	addr, ok := f.nodes[id]
	return addr, ok
}

func (f *FSM) ConfigNum() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.configNum
}

func (f *FSM) Serves(shard int) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.served[shard]
}

func (f *FSM) Pending() []int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]int, 0, len(f.pending))
	for s := range f.pending {
		out = append(out, s)
	}
	return out
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
	case OpFreeze:
		f.mu.Lock()
		f.configNum = cmd.Num
		for _, shard := range cmd.Remove {
			delete(f.served, shard)
		}
		for _, shard := range cmd.ServeNow {
			f.served[shard] = true
		}
		for _, shard := range cmd.Pending {
			f.pending[shard] = true
		}
		f.mu.Unlock()
	case OpInstall:
		f.mu.Lock()
		if cmd.Num == f.configNum {
			for k, v := range cmd.Data {
				if err := f.s.Set(k, v); err != nil {
					panic(fmt.Errorf("unable to apply install: %s", err))
				}
			}
			for shard := range f.pending {
				f.served[shard] = true
			}
			clear(f.pending)
		}
		f.mu.Unlock()
	case OpDrop:
		return f.drop(cmd)
	default:
		panic(fmt.Errorf("unrecognized command: %s", cmd.Op))

	}

	return nil
}

// drop deletes the keys of handed-off shards. It never drops a shard the node
// still serves, and it reports whether the node has caught up to the config
// that moved the shards away. A false return tells the new owner to retry.
func (f *FSM) drop(cmd Command) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cmd.Num > f.configNum {
		return false
	}
	toDrop := make(map[int]bool, len(cmd.Shards))
	for _, sh := range cmd.Shards {
		if !f.served[sh] {
			toDrop[sh] = true
		}
	}
	if len(toDrop) == 0 {
		return true
	}
	snap, ok := f.s.(store.Snapshotter)
	if !ok {
		return true
	}
	dump, err := snap.Dump()
	if err != nil {
		panic(fmt.Errorf("unable to dump for drop: %s", err))
	}
	for k := range dump {
		if toDrop[shard.Index(k, f.nShards)] {
			if err := f.s.Delete(k); err != nil {
				panic(fmt.Errorf("unable to apply drop: %s", err))
			}
		}
	}
	return true
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
	servedCopy := make(map[int]bool)
	maps.Copy(servedCopy, f.served)
	pendingCopy := make(map[int]bool)
	maps.Copy(pendingCopy, f.pending)

	return &Snapshot{KV: dump, Nodes: nodesCopy, ConfigNum: f.configNum, Served: servedCopy, Pending: pendingCopy}, nil

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
	f.configNum = snapshot.ConfigNum
	f.served = make(map[int]bool, len(snapshot.Served))
	maps.Copy(f.served, snapshot.Served)
	f.pending = make(map[int]bool, len(snapshot.Pending))
	maps.Copy(f.pending, snapshot.Pending)
	return nil
}

type Snapshot struct {
	KV        map[string]string
	Nodes     map[string]string
	ConfigNum int
	Served    map[int]bool
	Pending   map[int]bool
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if err := gob.NewEncoder(sink).Encode(s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {}
