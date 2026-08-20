package kvraft

import (
	"log/slog"
	"time"

	"github.com/fredouric/kvraft/raftgroup"
	"github.com/fredouric/kvraft/store"
)

var _ store.Store = (*Node)(nil)

const raftTimeout = 10 * time.Second

type Node struct {
	group      *raftgroup.Group
	innerStore store.Store
	fsm        *FSM
}

type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "node is not the leader; leader unknown"
	}
	return "node is not the leader; leader at " + e.LeaderAddr
}

func NewNode(store store.Store, localID string, bindAddr string, raftDir string) (*Node, error) {
	fsm := NewFSM(store)
	group, err := raftgroup.New(fsm, localID, bindAddr, raftDir)
	if err != nil {
		return nil, err
	}
	return &Node{group: group, innerStore: store, fsm: fsm}, nil
}

func (n *Node) Bootstrap() error {
	return n.group.Bootstrap()
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	return n.group.WaitForLeader(timeout)
}

func (n *Node) Close() error {
	return n.group.Close()
}

func (n *Node) Get(key string) (string, bool, error) {
	return n.innerStore.Get(key)
}

func (n *Node) Set(key string, value string) error {
	if !n.group.IsLeader() {
		return &NotLeaderError{LeaderAddr: n.leaderRPCAddr()}
	}
	return n.apply(Command{Op: OpSet, Key: key, Value: value})
}

func (n *Node) Delete(key string) error {
	if !n.group.IsLeader() {
		return &NotLeaderError{LeaderAddr: n.leaderRPCAddr()}
	}
	return n.apply(Command{Op: OpDelete, Key: key})
}

func (n *Node) apply(cmd Command) error {
	b, err := Encode(cmd)
	if err != nil {
		return err
	}
	_, err = n.group.Apply(b, raftTimeout)
	return err
}

func (n *Node) leaderRPCAddr() string {
	_, id := n.group.LeaderWithID()
	if id == "" {
		return ""
	}
	addr, _ := n.fsm.AddrFor(string(id))
	return addr
}

func (n *Node) Advertise(nodeID, rpcAddr string) error {
	return n.apply(Command{Op: OpAddNode, Key: nodeID, Value: rpcAddr})
}

func (n *Node) Join(nodeID, raftAddr, rpcAddr string) error {
	if err := n.group.AddVoter(nodeID, raftAddr); err != nil {
		return err
	}
	if err := n.Advertise(nodeID, rpcAddr); err != nil {
		return err
	}
	slog.Info("node joined successfully", "nodeID", nodeID, "raft", raftAddr, "rpc", rpcAddr)
	return nil
}
