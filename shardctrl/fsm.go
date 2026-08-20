package shardctrl

import (
	"encoding/gob"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/hashicorp/raft"
)

type FSM struct {
	mu      sync.RWMutex
	configs []Config
}

func NewFSM(nShards int) *FSM {
	return &FSM{configs: []Config{{
		Num:    0,
		Shards: make([]string, nShards),
		Groups: map[string][]string{},
	}}}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	cmd, err := Decode(log.Data)
	if err != nil {
		panic(fmt.Errorf("unable to decode data: %s", err))
	}

	switch cmd.Op {
	case OpQuery:
		f.mu.RLock()
		defer f.mu.RUnlock()
		return f.queryLocked(cmd.Num)
	case OpJoin:
		f.mu.Lock()
		defer f.mu.Unlock()
		groups := maps.Clone(f.lastLocked().Groups)
		for gid, members := range cmd.Servers {
			groups[gid] = members
		}
		f.rebalanceAppendLocked(groups)
	case OpLeave:
		f.mu.Lock()
		defer f.mu.Unlock()
		groups := maps.Clone(f.lastLocked().Groups)
		for _, gid := range cmd.GIDs {
			delete(groups, gid)
		}
		f.rebalanceAppendLocked(groups)
	case OpMove:
		f.mu.Lock()
		defer f.mu.Unlock()
		last := f.lastLocked()
		shards := slices.Clone(last.Shards)
		shards[cmd.Shard] = cmd.GID
		f.configs = append(f.configs, Config{
			Num:    last.Num + 1,
			Shards: shards,
			Groups: maps.Clone(last.Groups),
		})
	default:
		panic(fmt.Errorf("unrecognized command: %s", cmd.Op))
	}

	return nil
}

func (f *FSM) lastLocked() Config {
	return f.configs[len(f.configs)-1]
}

func (f *FSM) rebalanceAppendLocked(groups map[string][]string) {
	last := f.lastLocked()
	f.configs = append(f.configs, Config{
		Num:    last.Num + 1,
		Shards: rebalance(last.Shards, groups),
		Groups: groups,
	})
}

func (f *FSM) queryLocked(num int) Config {
	if num < 0 || num >= len(f.configs) {
		return f.configs[len(f.configs)-1]
	}
	return f.configs[num]
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &Snapshot{Configs: slices.Clone(f.configs)}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	snapshot := &Snapshot{}
	if err := gob.NewDecoder(rc).Decode(snapshot); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configs = snapshot.Configs
	return nil
}

type Snapshot struct {
	Configs []Config
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if err := gob.NewEncoder(sink).Encode(s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {}
