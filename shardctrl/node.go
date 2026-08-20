package shardctrl

import (
	"fmt"
	"time"

	"github.com/fredouric/kvraft/raftgroup"
)

const raftTimeout = 10 * time.Second

type Node struct {
	group *raftgroup.Group
	fsm   *FSM
}

type NotLeaderError struct {
	LeaderID string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "controller node is not the leader; leader unknown"
	}
	return "controller node is not the leader; leader is " + e.LeaderID
}

func NewNode(nShards int, localID, bindAddr, raftDir string) (*Node, error) {
	fsm := NewFSM(nShards)
	group, err := raftgroup.New(fsm, localID, bindAddr, raftDir)
	if err != nil {
		return nil, err
	}
	return &Node{group: group, fsm: fsm}, nil
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

// AddVoter adds another replica to the controller's own raft group.
func (n *Node) AddVoter(nodeID, raftAddr string) error {
	if !n.group.IsLeader() {
		return n.notLeader()
	}
	return n.group.AddVoter(nodeID, raftAddr)
}

// Join adds shard groups to the config and triggers a rebalance.
func (n *Node) Join(servers map[string][]string) error {
	return n.apply(Command{Op: OpJoin, Servers: servers})
}

func (n *Node) Leave(gids []string) error {
	return n.apply(Command{Op: OpLeave, GIDs: gids})
}

func (n *Node) Move(shard int, gid string) error {
	return n.apply(Command{Op: OpMove, Shard: shard, GID: gid})
}

func (n *Node) Query(num int) (Config, error) {
	if !n.group.IsLeader() {
		return Config{}, n.notLeader()
	}
	b, err := Encode(Command{Op: OpQuery, Num: num})
	if err != nil {
		return Config{}, err
	}
	resp, err := n.group.Apply(b, raftTimeout)
	if err != nil {
		return Config{}, err
	}
	cfg, ok := resp.(Config)
	if !ok {
		return Config{}, fmt.Errorf("unexpected query response type %T", resp)
	}
	return cfg, nil
}

func (n *Node) apply(cmd Command) error {
	if !n.group.IsLeader() {
		return n.notLeader()
	}
	b, err := Encode(cmd)
	if err != nil {
		return err
	}
	_, err = n.group.Apply(b, raftTimeout)
	return err
}

func (n *Node) notLeader() *NotLeaderError {
	_, id := n.group.LeaderWithID()
	return &NotLeaderError{LeaderID: string(id)}
}
