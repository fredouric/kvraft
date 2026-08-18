// Command kvclient is a thin CLI over the client package.
package main

import (
	"fmt"
	"os"

	"github.com/fredouric/kvraft/client"
	"github.com/fredouric/kvraft/shard"
)

const nShards = 256

func topology() (*shard.Config, error) {
	order := []string{"g1", "g2"}
	groups := map[string][]string{
		"g1": {"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082"},
		"g2": {"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092"},
	}
	shardGroups := make([]string, nShards)
	for i := range shardGroups {
		shardGroups[i] = order[i*len(order)/nShards]
	}
	return shard.New(shardGroups, groups)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: kvclient set <key> <value> | get <key> | delete <key>")
		os.Exit(2)
	}
	op := os.Args[1]

	cfg, err := topology()
	if err != nil {
		fail(err)
	}
	c := client.New(*cfg)
	defer c.Close()

	switch op {
	case "set":
		if err := c.Set(os.Args[2], os.Args[3]); err != nil {
			fail(err)
		}
		fmt.Println("set ok")
	case "delete":
		if err := c.Delete(os.Args[2]); err != nil {
			fail(err)
		}
		fmt.Println("delete ok")
	case "get":
		v, ok, err := c.Get(os.Args[2])
		if err != nil {
			fail(err)
		}
		fmt.Printf("%s = %q (found=%v)\n", os.Args[2], v, ok)
	default:
		fmt.Println("unknown op:", op)
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Println("error:", err)
	os.Exit(1)
}
