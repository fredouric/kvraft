package server

import (
	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/store/kvraft"
)

type ClusterService struct {
	Node *kvraft.Node
}

func (c *ClusterService) Join(args *kvapi.JoinArgs, reply *kvapi.Empty) error {
	return c.Node.Join(args.NodeID, args.RaftAddr)
}
