package shardctrl

import (
	"bytes"
	"encoding/gob"
)

type OpKind string

const OpJoin OpKind = "join"
const OpLeave OpKind = "leave"
const OpMove OpKind = "move"
const OpQuery OpKind = "query"

type Command struct {
	Op      OpKind
	Servers map[string][]string
	GIDs    []string
	Shard   int
	GID     string
	Num     int
}

func Encode(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte) (Command, error) {
	var cmd Command
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&cmd); err != nil {
		return Command{}, err
	}
	return cmd, nil
}
