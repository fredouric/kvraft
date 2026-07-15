package kvapi

type Empty struct{}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
	Found bool
}

type SetArgs struct {
	Key   string
	Value string
}

type DeleteArgs struct {
	Key string
}
