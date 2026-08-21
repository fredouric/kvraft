// Command kvclient is a thin CLI over the client package.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/fredouric/kvraft/client"
)

func main() {
	ctrlAddrs := flag.String("ctrl-addrs", "127.0.0.1:7080", "comma-separated controller RPC addresses")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("usage: kvclient [--ctrl-addrs a,b,c] set <key> <value> | get <key> | delete <key>")
		os.Exit(2)
	}
	op := args[0]

	addrs := strings.Split(*ctrlAddrs, ",")
	c, err := client.New(addrs)
	if err != nil {
		fail(err)
	}
	defer c.Close()

	switch op {
	case "set":
		if err := c.Set(args[1], args[2]); err != nil {
			fail(err)
		}
		fmt.Println("set ok")
	case "delete":
		if err := c.Delete(args[1]); err != nil {
			fail(err)
		}
		fmt.Println("delete ok")
	case "get":
		v, ok, err := c.Get(args[1])
		if err != nil {
			fail(err)
		}
		fmt.Printf("%s = %q (found=%v)\n", args[1], v, ok)
	default:
		fmt.Println("unknown op:", op)
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Println("error:", err)
	os.Exit(1)
}
