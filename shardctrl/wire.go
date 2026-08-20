package shardctrl

type AddNodeArgs struct {
	NodeID   string
	RaftAddr string
}

type JoinArgs struct {
	Servers map[string][]string
}

type LeaveArgs struct {
	GIDs []string
}

type MoveArgs struct {
	Shard int
	GID   string
}

type QueryArgs struct {
	Num int
}

type WriteReply struct {
	NotLeader bool
}

type QueryReply struct {
	Config    Config
	NotLeader bool
}
