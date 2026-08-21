package server

import (
	"errors"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/store/kvraft"
)

type MigrationService struct {
	Node *kvraft.Node
}

func (m *MigrationService) Pull(args *kvapi.PullArgs, reply *kvapi.PullReply) error {
	data, ready, err := m.Node.PullShards(args.Num, args.Shards)
	if err != nil {
		return err
	}
	reply.Ready = ready
	reply.Data = data
	return nil
}

func (m *MigrationService) Confirm(args *kvapi.ConfirmArgs, reply *kvapi.ConfirmReply) error {
	dropped, err := m.Node.Drop(args.Num, args.Shards)
	var nle *kvraft.NotLeaderError
	if errors.As(err, &nle) {
		reply.NotLeader = true
		reply.LeaderAddr = nle.LeaderAddr
		return nil
	}
	if err != nil {
		return err
	}
	reply.Dropped = dropped
	return nil
}
