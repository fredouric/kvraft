package shardctrl

import (
	"fmt"
	"net/rpc"
	"time"
)

const (
	ctrlMaxAttempts = 10
	ctrlRetryDelay  = 200 * time.Millisecond
)

type Client struct {
	addrs  []string
	leader int
	conn   *rpc.Client
}

func NewClient(addrs []string) *Client {
	return &Client{addrs: addrs}
}

func (c *Client) Close() error {
	return c.reset()
}

func (c *Client) dial() (*rpc.Client, error) {
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := rpc.DialHTTP("tcp", c.addrs[c.leader])
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *Client) reset() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) rotate() {
	c.reset()
	c.leader = (c.leader + 1) % len(c.addrs)
}

func (c *Client) do(method string, args any, reply any, notLeader func() bool) error {
	var lastErr error
	for attempt := 0; attempt < ctrlMaxAttempts; attempt++ {
		conn, err := c.dial()
		if err != nil {
			lastErr = err
			c.rotate()
			time.Sleep(ctrlRetryDelay)
			continue
		}
		if err := conn.Call(method, args, reply); err != nil {
			lastErr = err
			c.rotate()
			time.Sleep(ctrlRetryDelay)
			continue
		}
		if notLeader() {
			lastErr = fmt.Errorf("controller not leader")
			c.rotate()
			time.Sleep(ctrlRetryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf("%s failed after %d attempts: %w", method, ctrlMaxAttempts, lastErr)
}

func (c *Client) AddNode(nodeID, raftAddr string) error {
	var reply WriteReply
	return c.do("ControllerService.AddNode", &AddNodeArgs{NodeID: nodeID, RaftAddr: raftAddr}, &reply, func() bool { return reply.NotLeader })
}

func (c *Client) Join(servers map[string][]string) error {
	var reply WriteReply
	return c.do("ControllerService.Join", &JoinArgs{Servers: servers}, &reply, func() bool { return reply.NotLeader })
}

func (c *Client) Leave(gids []string) error {
	var reply WriteReply
	return c.do("ControllerService.Leave", &LeaveArgs{GIDs: gids}, &reply, func() bool { return reply.NotLeader })
}

func (c *Client) Move(shard int, gid string) error {
	var reply WriteReply
	return c.do("ControllerService.Move", &MoveArgs{Shard: shard, GID: gid}, &reply, func() bool { return reply.NotLeader })
}

func (c *Client) Query(num int) (Config, error) {
	var reply QueryReply
	if err := c.do("ControllerService.Query", &QueryArgs{Num: num}, &reply, func() bool { return reply.NotLeader }); err != nil {
		return Config{}, err
	}
	return reply.Config, nil
}
