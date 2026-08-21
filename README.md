# kvraft

A distributed, sharded, replicated key-value store in Go. It uses
`hashicorp/raft` for consensus and Go's `net/rpc` for the client API.

The keyspace is split into a fixed number of shards. Each shard belongs to one
replica group. Each replica group is a Raft cluster. A separate Raft group, the
shard controller, holds the sequence of cluster configurations and rebalances
shards when groups join or leave. Groups move shard data between themselves
while they serve traffic.

This is a learning project. It follows the layers in [ROADMAP.md](ROADMAP.md)
and the MIT 6.824 lab 3 and lab 4 path.

## Architecture

| Package | Role |
| --- | --- |
| `store` | The `Store` and `Snapshotter` interfaces. |
| `store/memory` | An in-memory store, used by tests. |
| `store/sqlite` | The on-disk store, used by the server. |
| `store/kvraft` | The KV `raft.FSM`, the command encoding, and the group node. |
| `raftgroup` | Shared Raft setup: transport, BoltDB log, snapshot store. |
| `shard` | The key-to-shard hash (`fnv32a % NShards`). |
| `shardctrl` | The controller Raft group, the config FSM, and its client. |
| `migrate` | The per-group loop that observes configs and moves shards. |
| `server` | The `net/rpc` services: KV, cluster, migration, controller. |
| `kvapi` | The wire types of the KV, cluster, and migration services. |
| `client` | The routing client. It caches the config and retries. |
| `cmd/kvclient` | A CLI over the client package. |

### Request path

1. The client asks the controller for the latest config.
2. The client hashes the key to a shard and looks up the owning group.
3. The client calls that group. It follows `NotLeader` to the leader.
4. On a `WrongGroup` reply, the client refetches the config and retries.

### Migration protocol

Each group leader runs a migrator. On every tick it does one step.

1. The migrator reads the next config from the controller.
2. It computes the shards to drop, the shards to serve at once, and the shards
   to pull.
3. It commits a `Freeze` command. All replicas of the group then agree on the
   new config number and stop serving the frozen shards.
4. It pulls the pending shard data from the previous owners.
5. It commits an `Install` command with the data. All replicas then apply the
   data and start to serve the new shards.

Freeze and install both go through the group's Raft log. So every replica
agrees on when the group owns a shard. `Install` checks the config number, so a
repeat apply is a no-op.

## Build

```sh
go build ./...
go test ./...
```

## Run a cluster

All commands run from the repository root. Every node must use the same
`--nshards` value. The default is 256.

### 1. Start the controller

```sh
go run . --id ctrl1 --controller --bootstrap \
  --raft-addr 127.0.0.1:7001 --rpc-addr 127.0.0.1:7080
```

Add more controller replicas with `--join`:

```sh
go run . --id ctrl2 --controller --join 127.0.0.1:7080 \
  --raft-addr 127.0.0.1:7002 --rpc-addr 127.0.0.1:7081
```

### 2. Start a replica group

Bootstrap the first node of group `g1`:

```sh
go run . --id g1n1 --group g1 --bootstrap \
  --raft-addr 127.0.0.1:9001 --rpc-addr 127.0.0.1:8080 \
  --ctrl-addrs 127.0.0.1:7080
```

Add two more nodes to the same group:

```sh
go run . --id g1n2 --group g1 --join 127.0.0.1:8080 \
  --raft-addr 127.0.0.1:9002 --rpc-addr 127.0.0.1:8081 \
  --ctrl-addrs 127.0.0.1:7080

go run . --id g1n3 --group g1 --join 127.0.0.1:8080 \
  --raft-addr 127.0.0.1:9003 --rpc-addr 127.0.0.1:8082 \
  --ctrl-addrs 127.0.0.1:7080
```

Start a second group `g2` the same way, on ports 9011-9013 and 8090-8092.

### 3. Register the groups with the controller

The controller has no admin CLI yet. Use the `shardctrl` client from a small Go
program:

```go
c := shardctrl.NewClient([]string{"127.0.0.1:7080"})
defer c.Close()

c.Join(map[string][]string{
    "g1": {"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082"},
})
```

Each `Join` appends a new config and rebalances the shards over all known
groups. `Leave` removes groups. `Move` sends one shard to one group. The
migrators pick up the new config and move the data.

### 4. Use the store

```sh
go run ./cmd/kvclient --ctrl-addrs 127.0.0.1:7080 set alpha 1
go run ./cmd/kvclient --ctrl-addrs 127.0.0.1:7080 get alpha
go run ./cmd/kvclient --ctrl-addrs 127.0.0.1:7080 delete alpha
```

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--id` | required | Unique node ID. |
| `--raft-addr` | `127.0.0.1:9001` | Address of the Raft transport. |
| `--rpc-addr` | `:8080` | Address of the client RPC API. |
| `--data-dir` | `data/<id>` | Directory for the Raft log, snapshots, and DB. |
| `--bootstrap` | `false` | Start a new Raft group with this node. |
| `--join` | none | RPC address of a node of the group to join. |
| `--controller` | `false` | Run this node as a shard controller. |
| `--nshards` | `256` | Number of shards. |
| `--group` | none | Shard group ID of this node. |
| `--ctrl-addrs` | none | Controller RPC addresses. |

A node without `--group` serves every key. It runs as a single unsharded
cluster. This keeps the Layer 2 setup usable.

