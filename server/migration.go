package server

import (
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
