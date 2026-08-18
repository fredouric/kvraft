package shard

import (
	"fmt"
	"hash/fnv"
)

type Config struct {
	NShards      int
	shardGroups  []string            // shard index -> group ID
	groupMembers map[string][]string // group ID -> members
}

func New(shardGroups []string, groupMembers map[string][]string) (*Config, error) {
	if len(shardGroups) == 0 {
		return nil, fmt.Errorf("shardGroups must not be empty")
	}
	for shard, group := range shardGroups {
		if group == "" {
			return nil, fmt.Errorf("shard %d has no group", shard)
		}
		if len(groupMembers[group]) == 0 {
			return nil, fmt.Errorf("shard %d maps to group %q, which has no members", shard, group)
		}
	}
	return &Config{
		NShards:      len(shardGroups),
		shardGroups:  shardGroups,
		groupMembers: groupMembers,
	}, nil
}

func (c *Config) GroupForKey(key string) (groupID string, members []string) {
	index := c.index(key)
	group := c.shardGroups[index]
	return group, c.groupMembers[group]
}

func (c *Config) index(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % c.NShards
}
