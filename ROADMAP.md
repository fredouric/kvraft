# Roadmap: Turning `kvraft` into a Distributed, Sharded, Replicated KV Store

> This is a **learning roadmap**, not an implementation plan. It explains *what* to build, *why*,
> and *which concepts to master at each step* — but leaves the code to you.
> Think of it as a syllabus. Build each layer and get it genuinely working before starting the next.

## Context

You have a single-process, in-memory state machine:
- `store.Store` (`store/store.go`) — the interface: `Get` / `Set` / `Delete`.
- `persisted.Store` (`store/persisted/store.go`) — a `map[string]string` under a `sync.Mutex`.
  (Note: named "persisted" but it does **not** write to disk yet — that name becomes true in Layer 2.)
- `main.go` — a `hello world` stub; nothing is networked.

**Goal:** a fault-tolerant, horizontally-scalable KV store = *sharding* (split the keyspace)
+ *replication via consensus* (each shard is a Raft group). This is the etcd/TiKV shape and the
MIT 6.824 lab 3 + lab 4 path.

**Chosen tools:** `hashicorp/raft` for consensus; Go's native `net/rpc` for the client API.

---

## The one mental model that makes everything click

A KV store is a **deterministic state machine**. If every replica:
1. starts in the same initial state, and
2. applies the **same sequence of commands in the same order**,

then every replica ends in the **same state**. So distributing the store reduces to one problem:
*get all replicas to agree on a single, ordered log of commands.* That agreement is **consensus**,
and Raft is the algorithm. `hashicorp/raft` gives you the agreement; **you** provide the state
machine that consumes the agreed-upon log. Everything below is a consequence of this idea.

---

## Layer 0 — Prerequisite reading (before writing anything)

Concepts to be able to explain in your own words:
- **Why replicate at all:** fault tolerance + availability; the cost is coordination.
- **Replicated State Machine (RSM)** model (the section above).
- **CAP / consistency spectrum:** what "linearizable" means and why it's the gold standard.
- **Raft's three sub-problems:** leader election, log replication, safety. Read the
  [Raft paper](https://raft.github.io/raft.pdf) (§5) and play with https://raft.github.io visualization.

**Checkpoint:** you can explain why a majority (quorum) is required and why an even number of nodes
buys you nothing over the odd number below it.

---

## Layer 1 — Networked single node (`net/rpc` client plane)

Turn the in-process store into a **server** clients reach over the network. No Raft yet.

Build:
- A TCP listener (`net.Listen("tcp", addr)`), then serve RPC on it via `rpc.Accept(listener)`
  (or register a handler with `rpc.HandleHTTP` + `http.Serve`).
- An **RPC service type** whose methods follow `net/rpc`'s required signature:
  `func (s *KV) Get(args *GetArgs, reply *GetReply) error` (and `Set`, `Delete`). Register it with
  `rpc.Register`. Define small `Args`/`Reply` structs — these are your wire types.
- Wire the RPC methods to your existing `store.Store`.

Concepts to master here:
- **`net/rpc` mechanics:** the exact method signature it requires (exported method, two pointer
  args, `error` return), how it gob-encodes args/replies, and that it handles framing and
  request/response matching **for you** (unlike raw TCP). Understand what you're *not* doing so you
  know where the abstraction boundary sits.
- **The client side:** `rpc.Dial("tcp", addr)` then `client.Call("KV.Set", &args, &reply)`. Sync
  vs async (`client.Go`) calls; how errors surface (transport error vs the `error` your method returns).
- Concurrency safety — `net/rpc` serves each request in its own goroutine, so your handlers hit
  the store concurrently; your existing mutex covers it, but *know* that's why it's needed.
- **Trade-off you accepted:** by using `net/rpc` you skip hand-rolling TCP framing (byte-stream →
  messages). That's fine — the distributed-systems learning is in Layers 2–4, not the framing. If
  you ever want the framing exercise, it's a self-contained side quest.

Design observations to fix while you're here:
- `Get` currently returns `("", nil)` for a missing key — decide on an explicit
  `ErrNotFound` so "empty value" and "absent key" are distinguishable. This matters later for
  correct replication semantics.
- `persisted.Store.store` map is never initialized — a real `Set` today would panic. Note it.

**Checkpoint:** connect a small Go client and set/get keys over RPC.

---

## Layer 2 — Replicate ONE group with `hashicorp/raft`

Make a **3-node cluster** that all hold the same data and survive one node dying. Still one group,
no sharding. This is the heart of the project.

The integration point is the **`raft.FSM` interface** — this is what you implement, and it *is*
the bridge between Raft and your `Store`:

```go
type FSM interface {
    Apply(*raft.Log) interface{}          // apply ONE committed command to your Store
    Snapshot() (raft.FSMSnapshot, error)  // capture current state for log compaction
    Restore(io.ReadCloser) error          // rebuild state from a snapshot
}
```

Things to understand and wire (the library provides the pieces; you assemble them):
- **Commands are opaque bytes to Raft.** You serialize your own op struct
  (`{Op, Key, Value}` via JSON/gob/protobuf), `raft.Apply(bytes, timeout)` on the **leader**,
  and *deserialize + execute* inside `FSM.Apply`. `Apply` MUST be deterministic — no clocks,
  no randomness, no map-iteration-order dependence.
- **Persistence (this is where "persisted" earns its name):** `raft-boltdb` provides the
  `LogStore` (the replicated log) and `StableStore` (term/vote metadata) on disk, so a node
  recovers its state after a crash. Plus a `SnapshotStore` (`raft.NewFileSnapshotStore`).
- **Transport:** `raft.NewTCPTransport` — the node↔node plane. Library-owned; you just give addresses.
- **Bootstrapping & membership:** `raft.BootstrapCluster` once, then `AddVoter` / `RemoveServer`.
- **Leadership routing:** only the leader may accept writes. On a follower, either reject with a
  redirect hint (`raft.Leader()`) or forward. Your client must retry against the new leader on
  leadership changes.
- **Reads & linearizability:** the subtle part. Reading the leader's local FSM can return **stale**
  data (a deposed leader doesn't know it yet). Options, in increasing correctness: read local
  (fast, stale) → `VerifyLeader()` before replying → route reads through the Raft log too. Start
  simple, then discuss the tradeoff.
- **Exactly-once / duplicate suppression:** clients retry, so the *same* command can reach the log
  twice. `Set`/`Delete` are idempotent so it's harmless now — but understand *why* non-idempotent
  ops (e.g. `Append`, `Incr`) require client-id + sequence-number dedup **inside the FSM**.
- **Snapshotting:** without it the log grows forever; `Snapshot`/`Restore` + `InstallSnapshot`
  let a lagging/new node catch up without replaying all history.

**Checkpoint:** start 3 nodes; write to the leader; kill the leader; confirm a new leader is
elected and the data survives; restart the dead node and watch it catch up.

---

## Layer 3 — Sharding (partition the keyspace)

Now scale out. Run **multiple replica groups**, each group (a Layer-2 Raft cluster) owning a subset
of the keyspace.

Concepts:
- **Shard assignment:** `shard = hash(key) % NShards` (fixed `NShards`, e.g. 256 or 1024). A shard
  is the unit of movement; keys never move individually. Understand why a *fixed, large* shard count
  beats hashing directly to groups (it makes rebalancing tractable — cf. consistent hashing).
- **Configuration = shard→group map.** Who decides which group owns which shard?

**Checkpoint (naive):** with a *static* hand-written config, a client hashes the key, looks up the
owning group, and talks to that group's leader. Two groups each serving half the shards.

---

## Layer 4 — The shard controller + live migration (the hard part)

Static config isn't distributed operations. Add a **shard controller**: a small, *separate Raft
group* whose FSM stores the sequence of cluster **configurations** (shard→group maps). Groups and
clients query it for the current config.

The genuinely hard problem — **shard migration during reconfiguration**:
- When config N→N+1 moves shard S from group A to group B, B must **pull S's data from A** and A
  must **stop serving S** — without losing writes or breaking linearizability.
- The migration itself must go **through each group's Raft log** (so all replicas of a group agree
  on when they own/disown a shard), and must be idempotent (configs can be re-observed).
- Requests for a shard you don't currently own must be rejected (client retries against new owner
  after re-reading config), and in-flight moves must not double-apply.

This is MIT 6.824 lab 4B — expect it to be the most subtle code you write. Do not start it until
Layers 2 and 3 are rock-solid.

**Checkpoint:** add a new group to a running cluster; watch shards rebalance onto it; confirm no
key is lost or served by two groups at once during the move.

---

## Suggested repo shape (as it grows)

- `store/` — your deterministic state machine (keep the `Store` interface; it becomes the FSM's core).
- `server/` — `net/rpc` client-plane service + Args/Reply types (Layer 1).
- `raftkv/` — the `raft.FSM` impl, op serialization, node bootstrap/membership (Layer 2).
- `shard/` — sharding logic, client-side routing (Layer 3).
- `shardctrl/` — the controller Raft group + config FSM (Layer 4).
- `client/` — a small client library that hashes keys, caches config, and retries on
  leader/owner changes.

Add dependencies (`hashicorp/raft`, `hashicorp/raft-boltdb`) to `go.mod` only when you reach Layer 2.

---

## How to verify each layer (end-to-end, not just unit tests)

- **Layer 1:** write a tiny Go client (`rpc.Dial` + `client.Call`) that does a few SET/GET/DEL;
  a table-driven test asserting round-trips. (`nc`/`telnet` won't work — `net/rpc` speaks gob, not text.)
- **Layer 2:** multi-node local cluster (different ports/data dirs). A **fault-injection drill**:
  write → `kill -9` the leader → verify re-election and data survival → restart → verify catch-up.
  This is the single most important test in the whole project.
- **Layer 3:** assert keys land on the expected group; measure that two groups share load.
- **Layer 4:** reconfigure live and assert *linearizability* holds across the move — a
  read after a write always sees the write, even mid-migration. Consider a small linearizability
  checker (or the porcupine library) as a stretch goal.

---

## Order of operations (do not skip ahead)

1. Layer 1 — networked single node over `net/rpc`.
2. Layer 2 — one 3-node Raft group (fault tolerance). **Spend the most time here.**
3. Layer 3 — static sharding across groups.
4. Layer 4 — shard controller + live migration.

Each layer is independently demoable. If you get stuck, the fastest unblock is almost always to
re-read the relevant Raft paper section and add logging around `FSM.Apply` and leadership changes.
