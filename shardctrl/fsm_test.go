package shardctrl

import (
	"slices"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCmd(t *testing.T, fsm *FSM, cmd Command) interface{} {
	t.Helper()
	b, err := Encode(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return fsm.Apply(&raft.Log{Data: b})
}

func counts(shards []string) map[string]int {
	c := map[string]int{}
	for _, g := range shards {
		c[g]++
	}
	return c
}

func TestRebalanceBalanced(t *testing.T) {
	groups := map[string][]string{"g1": {"a"}, "g2": {"b"}, "g3": {"c"}}
	shards := rebalance(make([]string, 10), groups)

	c := counts(shards)
	if c[""] != 0 {
		t.Fatalf("unassigned shards remain: %d", c[""])
	}
	for _, g := range []string{"g1", "g2", "g3"} {
		if c[g] < 3 || c[g] > 4 {
			t.Fatalf("group %s owns %d shards, want 3 or 4", g, c[g])
		}
	}
}

func TestRebalanceDeterministic(t *testing.T) {
	groups := map[string][]string{"g1": {"a"}, "g2": {"b"}, "g3": {"c"}, "g4": {"d"}}
	a := rebalance(make([]string, 256), groups)
	b := rebalance(make([]string, 256), groups)
	if !slices.Equal(a, b) {
		t.Fatal("rebalance is not deterministic")
	}
}

func TestRebalanceMinimalMovement(t *testing.T) {
	three := map[string][]string{"g1": {"a"}, "g2": {"b"}, "g3": {"c"}}
	before := rebalance(make([]string, 256), three)

	four := map[string][]string{"g1": {"a"}, "g2": {"b"}, "g3": {"c"}, "g4": {"d"}}
	after := rebalance(slices.Clone(before), four)

	moved := 0
	for i := range before {
		if before[i] != after[i] {
			moved++
		}
	}
	if moved > 256/4+1 {
		t.Fatalf("moved %d shards, want about %d", moved, 256/4)
	}
	if counts(after)["g4"] == 0 {
		t.Fatal("g4 received no shards")
	}
}

func TestFSMApplyFlow(t *testing.T) {
	fsm := NewFSM(6)

	applyCmd(t, fsm, Command{Op: OpJoin, Servers: map[string][]string{"g1": {"a"}, "g2": {"b"}}})
	cfg := applyCmd(t, fsm, Command{Op: OpQuery, Num: -1}).(Config)
	if cfg.Num != 1 {
		t.Fatalf("after join, config num = %d, want 1", cfg.Num)
	}
	if counts(cfg.Shards)[""] != 0 {
		t.Fatal("shards left unassigned after join")
	}

	applyCmd(t, fsm, Command{Op: OpMove, Shard: 0, GID: "g2"})
	cfg = applyCmd(t, fsm, Command{Op: OpQuery, Num: -1}).(Config)
	if cfg.Num != 2 || cfg.Shards[0] != "g2" {
		t.Fatalf("after move, num=%d shard0=%q, want 2 g2", cfg.Num, cfg.Shards[0])
	}

	applyCmd(t, fsm, Command{Op: OpLeave, GIDs: []string{"g1"}})
	cfg = applyCmd(t, fsm, Command{Op: OpQuery, Num: -1}).(Config)
	if slices.Contains(cfg.Shards, "g1") {
		t.Fatal("g1 still owns shards after leave")
	}
	if counts(cfg.Shards)[""] != 0 {
		t.Fatal("shards left unassigned after leave")
	}

	old := applyCmd(t, fsm, Command{Op: OpQuery, Num: 1}).(Config)
	if old.Num != 1 {
		t.Fatalf("historical query returned num %d, want 1", old.Num)
	}
}
