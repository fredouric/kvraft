package client

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/shard"
	"github.com/fredouric/kvraft/shardctrl"
)

const (
	maxAttempts = 5
	retryDelay  = 300 * time.Millisecond
)

type Client struct {
	ctrl    *shardctrl.Client
	cfg     shardctrl.Config
	leaders map[string]string
	conns   map[string]*rpc.Client
}

func New(ctrlAddrs []string) (*Client, error) {
	c := &Client{
		ctrl:    shardctrl.NewClient(ctrlAddrs),
		leaders: make(map[string]string),
		conns:   make(map[string]*rpc.Client),
	}
	if err := c.refresh(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) Close() {
	for group := range c.conns {
		c.reset(group)
	}
	c.ctrl.Close()
}

func (c *Client) refresh() error {
	cfg, err := c.ctrl.Query(-1)
	if err != nil {
		return err
	}
	c.cfg = cfg
	return nil
}

func (c *Client) route(key string) (groupID string, members []string) {
	if len(c.cfg.Shards) == 0 {
		return "", nil
	}
	index := shard.Index(key, len(c.cfg.Shards))
	groupID = c.cfg.Shards[index]
	return groupID, c.cfg.Groups[groupID]
}

func (c *Client) dial(groupID string, members []string) (*rpc.Client, error) {
	if conn := c.conns[groupID]; conn != nil {
		return conn, nil
	}
	addr := c.leaders[groupID]
	if addr == "" {
		if len(members) == 0 {
			return nil, fmt.Errorf("no members for group %q", groupID)
		}
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
	return c.write("KVService.Set", key, &kvapi.SetArgs{Key: key, Value: value})
}

func (c *Client) Delete(key string) error {
	return c.write("KVService.Delete", key, &kvapi.DeleteArgs{Key: key})
}

func (c *Client) write(method, key string, args any) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		group, members := c.route(key)
		if group == "" {
			lastErr = fmt.Errorf("no group serves key %q", key)
			c.refresh()
			time.Sleep(retryDelay)
			continue
		}

		conn, err := c.dial(group, members)
		if err != nil {
			lastErr = err
			c.reset(group)
			time.Sleep(retryDelay)
			continue
		}

		var reply kvapi.WriteReply
		if err := conn.Call(method, args, &reply); err != nil {
			lastErr = err
			c.reset(group)
			time.Sleep(retryDelay)
			continue
		}

		if reply.NotLeader {
			if reply.LeaderAddr != "" {
				c.leaders[group] = reply.LeaderAddr
			}
			c.reset(group)
			time.Sleep(retryDelay)
			continue
		}

		if reply.WrongGroup {
			lastErr = fmt.Errorf("group %q no longer serves key %q", group, key)
			c.refresh()
			time.Sleep(retryDelay)
			continue
		}

		return nil
	}
	return fmt.Errorf("%s failed after %d attempts: %w", method, maxAttempts, lastErr)
}

func (c *Client) Get(key string) (string, bool, error) {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		group, members := c.route(key)
		if group == "" {
			lastErr = fmt.Errorf("no group serves key %q", key)
			c.refresh()
			time.Sleep(retryDelay)
			continue
		}

		conn, err := c.dial(group, members)
		if err != nil {
			lastErr = err
			c.reset(group)
			time.Sleep(retryDelay)
			continue
		}

		var reply kvapi.GetReply
		if err := conn.Call("KVService.Get", &kvapi.GetArgs{Key: key}, &reply); err != nil {
			lastErr = err
			c.reset(group)
			time.Sleep(retryDelay)
			continue
		}

		if reply.WrongGroup {
			lastErr = fmt.Errorf("group %q no longer serves key %q", group, key)
			c.refresh()
			time.Sleep(retryDelay)
			continue
		}

		return reply.Value, reply.Found, nil
	}
	return "", false, fmt.Errorf("get failed after %d attempts: %w", maxAttempts, lastErr)
}
