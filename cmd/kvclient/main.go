// Command kvclient is a thin CLI over the client package.
//
//	kvclient <addr> set <key> <value>
//	kvclient <addr> get <key>
//	kvclient <addr> delete <key>
package main

import (
	"fmt"
	"os"

	"github.com/fredouric/kvraft/client"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: kvclient <addr> set <key> <value> | get <key> | delete <key>")
		os.Exit(2)
	}
	addr, op := os.Args[1], os.Args[2]

	c := client.New(addr)
	defer c.Close()

	switch op {
	case "set":
		if err := c.Set(os.Args[3], os.Args[4]); err != nil {
			fail(err)
		}
		fmt.Println("set ok")
	case "delete":
		if err := c.Delete(os.Args[3]); err != nil {
			fail(err)
		}
		fmt.Println("delete ok")
	case "get":
		v, ok, err := c.Get(os.Args[3])
		if err != nil {
			fail(err)
		}
		fmt.Printf("%s = %q (found=%v)\n", os.Args[3], v, ok)
	default:
		fmt.Println("unknown op:", op)
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Println("error:", err)
	os.Exit(1)
}
