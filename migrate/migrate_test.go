package migrate

import (
	"reflect"
	"testing"

	"github.com/fredouric/kvraft/shardctrl"
)

func TestDiff(t *testing.T) {
	// shard0: gained from empty -> serveNow
	// shard1: owned before and after -> unchanged
	// shard2: gained from another group -> pending
	// shard3: lost -> remove
	prev := shardctrl.Config{Shards: []string{"", "g1", "g2", "g1"}}
	next := shardctrl.Config{Shards: []string{"g1", "g1", "g1", "g2"}}

	remove, serveNow, pending := diff(prev, next, "g1")

	if want := []int{3}; !reflect.DeepEqual(remove, want) {
		t.Errorf("remove = %v, want %v", remove, want)
	}
	if want := []int{0}; !reflect.DeepEqual(serveNow, want) {
		t.Errorf("serveNow = %v, want %v", serveNow, want)
	}
	if want := []int{2}; !reflect.DeepEqual(pending, want) {
		t.Errorf("pending = %v, want %v", pending, want)
	}
}
