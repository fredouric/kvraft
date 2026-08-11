package client

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/fredouric/kvraft/kvapi"
)

const (
	maxAttempts = 5
	retryDelay  = 300 * time.Millisecond
)

type Client struct {
	addr string
	conn *rpc.Client
}

func New(addr string) *Client {
	return &Client{addr: addr}
}

func (c *Client) Close() error {
	return c.reset()
}

func (c *Client) dial() (*rpc.Client, error) {
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := rpc.DialHTTP("tcp", c.addr)
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

func (c *Client) Set(key, value string) error {
	return c.write("KVService.Set", &kvapi.SetArgs{Key: key, Value: value})
}

func (c *Client) Delete(key string) error {
	return c.write("KVService.Delete", &kvapi.DeleteArgs{Key: key})
}

func (c *Client) write(method string, args any) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		conn, err := c.dial()
		if err != nil {
			lastErr = err
			c.reset()
			time.Sleep(retryDelay)
			continue
		}

		var reply kvapi.WriteReply
		if err := conn.Call(method, args, &reply); err != nil {
			lastErr = err
			c.reset()
			time.Sleep(retryDelay)
			continue
		}

		if reply.NotLeader {
			if reply.LeaderAddr != "" {
				c.addr = reply.LeaderAddr
			}
			c.reset()
			time.Sleep(retryDelay)
			continue
		}

		return nil
	}
	return fmt.Errorf("%s failed after %d attempts: %w", method, maxAttempts, lastErr)
}

func (c *Client) Get(key string) (string, bool, error) {
	conn, err := c.dial()
	if err != nil {
		return "", false, err
	}
	var reply kvapi.GetReply
	if err := conn.Call("KVService.Get", &kvapi.GetArgs{Key: key}, &reply); err != nil {
		c.reset()
		return "", false, err
	}
	return reply.Value, reply.Found, nil
}
