package kvapi

type Empty struct{}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value      string
	Found      bool
	WrongGroup bool
}

type SetArgs struct {
	Key   string
	Value string
}

type DeleteArgs struct {
	Key string
}

type WriteReply struct {
	NotLeader  bool
	LeaderAddr string
	WrongGroup bool
}

type JoinArgs struct {
	NodeID   string
	RaftAddr string
	RpcAddr  string
}

type PullArgs struct {
	Num    int
	Shards []int
}

type PullReply struct {
	Ready bool
	Data  map[string]string
}

type ConfirmArgs struct {
	Num    int
	Shards []int
}

type ConfirmReply struct {
	Dropped    bool
	NotLeader  bool
	LeaderAddr string
}
