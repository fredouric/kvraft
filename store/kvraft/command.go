package kvraft

import (
	"bytes"
	"encoding/gob"
)

type OpKind string

const OpSet OpKind = "set"
const OpDelete OpKind = "delete"
const OpAddNode OpKind = "addnode"
const OpFreeze OpKind = "freeze"
const OpInstall OpKind = "install"

type Command struct {
	Op    OpKind
	Key   string
	Value string

	Num      int
	Remove   []int
	ServeNow []int
	Pending  []int
	Data     map[string]string
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
