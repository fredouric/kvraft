package migrate

import (
	"context"
	"net/rpc"
	"time"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/shardctrl"
	"github.com/fredouric/kvraft/store/kvraft"
)

const (
	pollInterval   = 100 * time.Millisecond
	pullRetryDelay = 100 * time.Millisecond
)

type Migrator struct {
	node    *kvraft.Node
	ctrl    *shardctrl.Client
	group   string
	nShards int
}

func New(node *kvraft.Node, ctrl *shardctrl.Client, group string, nShards int) *Migrator {
	return &Migrator{node: node, ctrl: ctrl, group: group, nShards: nShards}
}

func (m *Migrator) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.step(ctx)
		}
	}
}

func (m *Migrator) step(ctx context.Context) {
	if !m.node.IsLeader() {
		return
	}
	cur := m.node.ConfigNum()

	if pending := m.node.Pending(); len(pending) > 0 {
		prev, err := m.ctrl.Query(cur - 1)
		if err != nil {
			return
		}
		data, err := m.pullAll(ctx, prev, pending, cur)
		if err != nil {
			return
		}
		m.node.Install(cur, data)
		return
	}

	latest, err := m.ctrl.Query(-1)
	if err != nil {
		return
	}
	if cur >= latest.Num {
		return
	}

	prev, err := m.ctrl.Query(cur)
	if err != nil {
		return
	}
	next, err := m.ctrl.Query(cur + 1)
	if err != nil {
		return
	}

	remove, serveNow, pending := diff(prev, next, m.group)
	m.node.Freeze(cur+1, remove, serveNow, pending)
}

func (m *Migrator) pullAll(ctx context.Context, prev shardctrl.Config, pending []int, num int) (map[string]string, error) {
	byOwner := map[string][]int{}
	for _, s := range pending {
		owner := prev.Shards[s]
		byOwner[owner] = append(byOwner[owner], s)
	}

	data := map[string]string{}
	for owner, shards := range byOwner {
		d, err := pullFromGroup(ctx, prev.Groups[owner], num, shards)
		if err != nil {
			return nil, err
		}
		for k, v := range d {
			data[k] = v
		}
	}
	return data, nil
}

func pullFromGroup(ctx context.Context, members []string, num int, shards []int) (map[string]string, error) {
	args := &kvapi.PullArgs{Num: num, Shards: shards}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		addr := members[attempt%len(members)]
		conn, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			if !sleep(ctx, pullRetryDelay) {
				return nil, ctx.Err()
			}
			continue
		}
		var reply kvapi.PullReply
		err = conn.Call("MigrationService.Pull", args, &reply)
		conn.Close()
		if err != nil || !reply.Ready {
			if !sleep(ctx, pullRetryDelay) {
				return nil, ctx.Err()
			}
			continue
		}
		return reply.Data, nil
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func diff(prev, next shardctrl.Config, group string) (remove, serveNow, pending []int) {
	for s := range next.Shards {
		ownedPrev := prev.Shards[s] == group
		ownedNext := next.Shards[s] == group
		switch {
		case ownedNext && !ownedPrev:
			if prev.Shards[s] == "" {
				serveNow = append(serveNow, s)
			} else {
				pending = append(pending, s)
			}
		case ownedPrev && !ownedNext:
			remove = append(remove, s)
		}
	}
	return remove, serveNow, pending
}
