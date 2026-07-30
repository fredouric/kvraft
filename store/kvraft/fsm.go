package kvraft

import (
	"errors"
	"fmt"
	"io"

	"github.com/fredouric/kvraft/store"
	"github.com/hashicorp/raft"
)

type FSM struct {
	s store.Store
}

func NewFSM(s store.Store) *FSM {
	return &FSM{s: s}
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
	default:
		panic(fmt.Errorf("unrecognized command: %s", cmd.Op))

	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, errors.New("snapshot not implemented")
}
func (f *FSM) Restore(rc io.ReadCloser) error { return errors.New("restore not implemented") }
