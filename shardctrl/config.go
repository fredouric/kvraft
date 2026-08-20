package shardctrl

import "sort"

type Config struct {
	Num    int
	Shards []string
	Groups map[string][]string
}

func rebalance(shards []string, groups map[string][]string) []string {
	if len(groups) == 0 {
		return make([]string, len(shards))
	}
	gids := make([]string, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Strings(gids)
	G := len(gids)
	base := len(shards) / G
	rem := len(shards) % G
	target := make(map[string]int, G)
	for i, gid := range gids {
		target[gid] = base
		if i < rem {
			target[gid]++
		}
	}
	result := make([]string, len(shards))

	count := make(map[string]int, G)
	var free []int
	for i, gid := range shards {
		if _, ok := groups[gid]; ok {
			result[i] = gid
			count[gid]++
		} else {
			free = append(free, i)
		}
	}
	for _, gid := range gids {
		for i := 0; i < len(shards) && count[gid] > target[gid]; i++ {
			if result[i] == gid {
				result[i] = ""
				free = append(free, i)
				count[gid]--
			}
		}
	}

	fi := 0
	for _, gid := range gids {
		for count[gid] < target[gid] {
			result[free[fi]] = gid
			count[gid]++
			fi++
		}
	}

	return result
}
