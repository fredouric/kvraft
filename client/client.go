package client

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/shard"
)

const (
	maxAttempts = 5
	retryDelay  = 300 * time.Millisecond
)

type Client struct {
	cfg     *shard.Config
	leaders map[string]string
	conns   map[string]*rpc.Client
}

func New(cfg shard.Config) *Client {
	return &Client{cfg: &cfg, leaders: make(map[string]string), conns: make(map[string]*rpc.Client)}
}

func (c *Client) Close() {
	for group := range c.conns {
		c.reset(group)
	}
}

func (c *Client) dial(groupID string, members []string) (*rpc.Client, error) {
	if conn := c.conns[groupID]; conn != nil {
		return conn, nil
	}
	addr := c.leaders[groupID]
	if addr == "" {
		addr = members[0]
	}
	conn, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.conns[groupID] = conn
	return conn, nil
}

func (c *Client) reset(groupID string) {
	if conn := c.conns[groupID]; conn != nil {
		conn.Close()
		delete(c.conns, groupID)
	}
}

func (c *Client) Set(key, value string) error {
	group, members := c.cfg.GroupForKey(key)
	return c.write(group, members, "KVService.Set", &kvapi.SetArgs{Key: key, Value: value})
}

func (c *Client) Delete(key string) error {
	group, members := c.cfg.GroupForKey(key)
	return c.write(group, members, "KVService.Delete", &kvapi.DeleteArgs{Key: key})
}

func (c *Client) write(groupID string, members []string, method string, args any) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		conn, err := c.dial(groupID, members)
		if err != nil {
			lastErr = err
			c.reset(groupID)
			time.Sleep(retryDelay)
			continue
		}

		var reply kvapi.WriteReply
		if err := conn.Call(method, args, &reply); err != nil {
			lastErr = err
			c.reset(groupID)
			time.Sleep(retryDelay)
			continue
		}

		if reply.NotLeader {
			if reply.LeaderAddr != "" {
				c.leaders[groupID] = reply.LeaderAddr
			}
			c.reset(groupID)
			time.Sleep(retryDelay)
			continue
		}

		return nil
	}
	return fmt.Errorf("%s failed after %d attempts: %w", method, maxAttempts, lastErr)
}

func (c *Client) Get(key string) (string, bool, error) {
	group, members := c.cfg.GroupForKey(key)
	conn, err := c.dial(group, members)
	if err != nil {
		return "", false, err
	}
	var reply kvapi.GetReply
	if err := conn.Call("KVService.Get", &kvapi.GetArgs{Key: key}, &reply); err != nil {
		c.reset(group)
		return "", false, err
	}
	return reply.Value, reply.Found, nil
}
