package migrate_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/fredouric/kvraft/client"
	"github.com/fredouric/kvraft/migrate"
	"github.com/fredouric/kvraft/server"
	"github.com/fredouric/kvraft/shard"
	"github.com/fredouric/kvraft/shardctrl"
	"github.com/fredouric/kvraft/store/kvraft"
	"github.com/fredouric/kvraft/store/memory"
)

const nShards = 4

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}

// startController starts a single-node controller and returns its RPC address.
func startController(t *testing.T) string {
	t.Helper()
	node, err := shardctrl.NewNode(nShards, "ctrl1", freeAddr(t), t.TempDir())
	if err != nil {
		t.Fatalf("controller NewNode: %v", err)
	}
	if err := node.Bootstrap(); err != nil {
		t.Fatalf("controller Bootstrap: %v", err)
	}
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("controller WaitForLeader: %v", err)
	}
	srv := server.New("127.0.0.1:0", &server.ControllerService{Node: node})
	if err := srv.Listen(); err != nil {
		t.Fatalf("controller Listen: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		node.Close()
	})
	return srv.Addr()
}

type kvGroup struct {
	node    *kvraft.Node
	rpcAddr string
}

// startGroup starts a single-node shard group with a running migrator.
func startGroup(t *testing.T, id, group string, ctrlAddrs []string) *kvGroup {
	t.Helper()
	store := memory.New()
	node, err := kvraft.NewNode(store, id, freeAddr(t), t.TempDir(), nShards, true)
	if err != nil {
		t.Fatalf("group NewNode: %v", err)
	}
	if err := node.Bootstrap(); err != nil {
		t.Fatalf("group Bootstrap: %v", err)
	}
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("group WaitForLeader: %v", err)
	}

	srv := server.New("127.0.0.1:0", &server.KVService{Store: node}, &server.MigrationService{Node: node})
	if err := srv.Listen(); err != nil {
		t.Fatalf("group Listen: %v", err)
	}
	rpcAddr := srv.Addr()
	if err := node.Advertise(id, rpcAddr); err != nil {
		t.Fatalf("group Advertise: %v", err)
	}

	ctrl := shardctrl.NewClient(ctrlAddrs)
	mig := migrate.New(node, ctrl, group, nShards)
	ctx, cancel := context.WithCancel(context.Background())
	go mig.Run(ctx)

	t.Cleanup(func() {
		cancel()
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		srv.Shutdown(sctx)
		node.Close()
		ctrl.Close()
	})
	return &kvGroup{node: node, rpcAddr: rpcAddr}
}

// TestJoinTriggersPull starts one group, writes data, then joins a second
// group. The rebalance moves shards to the new group, which must pull their
// data. The client must still read every key afterward.
func TestJoinTriggersPull(t *testing.T) {
	ctrlAddr := startController(t)
	ctrlAddrs := []string{ctrlAddr}
	admin := shardctrl.NewClient(ctrlAddrs)
	defer admin.Close()

	g1 := startGroup(t, "g1node", "g1", ctrlAddrs)

	// Join g1 alone. It serves all shards from empty, so no pull happens.
	if err := admin.Join(map[string][]string{"g1": {g1.rpcAddr}}); err != nil {
		t.Fatalf("Join g1: %v", err)
	}
	eventually(t, 5*time.Second, func() bool { return g1.node.ConfigNum() == 1 }, "g1 did not reach config 1")

	c, err := client.New(ctrlAddrs)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	defer c.Close()

	keys := map[string]string{}
	for i := 0; i < 20; i++ {
		k := "key" + string(rune('a'+i))
		v := "val" + string(rune('a'+i))
		keys[k] = v
		if err := c.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	// Join g2. The controller rebalances, and g2 must pull the moved shards.
	g2 := startGroup(t, "g2node", "g2", ctrlAddrs)
	if err := admin.Join(map[string][]string{"g1": {g1.rpcAddr}, "g2": {g2.rpcAddr}}); err != nil {
		t.Fatalf("Join g2: %v", err)
	}

	eventually(t, 10*time.Second, func() bool {
		return g1.node.ConfigNum() == 2 && g2.node.ConfigNum() == 2
	}, "groups did not reach config 2 after second join")

	// Every key must still be readable, even the ones that moved to g2.
	eventually(t, 10*time.Second, func() bool {
		for k, want := range keys {
			v, ok, err := c.Get(k)
			if err != nil || !ok || v != want {
				return false
			}
		}
		return true
	}, "not every key was readable after the join and pull")

	// Prove g2 really serves and holds pulled data for at least one key.
	cfg, err := admin.Query(-1)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	served := false
	for k := range keys {
		if cfg.Shards[shard.Index(k, nShards)] == "g2" {
			if _, ok, _ := g2.node.Get(k); ok {
				served = true
				break
			}
		}
	}
	if !served {
		t.Fatal("g2 served no pulled key; the join did not trigger a real pull")
	}
}

// TestMove writes a key, then moves its shard to the other group. The new
// owner must pull the key's data, and the client must read it back.
func TestMove(t *testing.T) {
	ctrlAddr := startController(t)
	ctrlAddrs := []string{ctrlAddr}
	admin := shardctrl.NewClient(ctrlAddrs)
	defer admin.Close()

	g1 := startGroup(t, "g1node", "g1", ctrlAddrs)
	g2 := startGroup(t, "g2node", "g2", ctrlAddrs)

	if err := admin.Join(map[string][]string{"g1": {g1.rpcAddr}, "g2": {g2.rpcAddr}}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	eventually(t, 5*time.Second, func() bool {
		return g1.node.ConfigNum() == 1 && g2.node.ConfigNum() == 1
	}, "groups did not reach config 1")

	c, err := client.New(ctrlAddrs)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	defer c.Close()

	const key, value = "movekey", "movevalue"
	if err := c.Set(key, value); err != nil {
		t.Fatalf("Set: %v", err)
	}

	cfg, err := admin.Query(-1)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	sh := shard.Index(key, nShards)
	from := cfg.Shards[sh]
	to := "g1"
	if from == "g1" {
		to = "g2"
	}

	if err := admin.Move(sh, to); err != nil {
		t.Fatalf("Move: %v", err)
	}

	// After the move, the client reroutes to the new owner and reads the
	// value that the new owner pulled from the old owner.
	eventually(t, 10*time.Second, func() bool {
		v, ok, err := c.Get(key)
		return err == nil && ok && v == value
	}, "value did not survive the move; the new owner did not pull the data")
}
