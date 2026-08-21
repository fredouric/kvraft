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
	index := c.Index(key)
	group := c.shardGroups[index]
	return group, c.groupMembers[group]
}

func (c *Config) Index(key string) int {
	return Index(key, c.NShards)
}

func Index(key string, nShards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % nShards
}
