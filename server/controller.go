package server

import (
	"errors"

	"github.com/fredouric/kvraft/shardctrl"
)

type ControllerService struct {
	Node *shardctrl.Node
}

func (c *ControllerService) AddNode(args *shardctrl.AddNodeArgs, reply *shardctrl.WriteReply) error {
	return ctrlWrite(c.Node.AddVoter(args.NodeID, args.RaftAddr), reply)
}

func (c *ControllerService) Join(args *shardctrl.JoinArgs, reply *shardctrl.WriteReply) error {
	return ctrlWrite(c.Node.Join(args.Servers), reply)
}

func (c *ControllerService) Leave(args *shardctrl.LeaveArgs, reply *shardctrl.WriteReply) error {
	return ctrlWrite(c.Node.Leave(args.GIDs), reply)
}

func (c *ControllerService) Move(args *shardctrl.MoveArgs, reply *shardctrl.WriteReply) error {
	return ctrlWrite(c.Node.Move(args.Shard, args.GID), reply)
}

func (c *ControllerService) Query(args *shardctrl.QueryArgs, reply *shardctrl.QueryReply) error {
	cfg, err := c.Node.Query(args.Num)
	var nle *shardctrl.NotLeaderError
	if errors.As(err, &nle) {
		reply.NotLeader = true
		return nil
	}
	if err != nil {
		return err
	}
	reply.Config = cfg
	return nil
}

func ctrlWrite(err error, reply *shardctrl.WriteReply) error {
	var nle *shardctrl.NotLeaderError
	if errors.As(err, &nle) {
		reply.NotLeader = true
		return nil
	}
	return err
}
