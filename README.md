<div align="center">

<img src="docs/media/beedb-icon.png" width="160" alt="BeeDB logo: a cartoon bee">

# BeeDB

**A distributed key-value store in Java 21. It speaks the memcached text protocol, and it replicates writes with a Raft implementation written from scratch.**

[![CI](https://github.com/codecasualty/beedb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/codecasualty/beedb/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
![Java 21](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)
![Maven](https://img.shields.io/badge/build-Maven-C71A36?logo=apachemaven&logoColor=white)
![Spring Boot 3.3](https://img.shields.io/badge/gateway-Spring%20Boot%203.3-6DB33F?logo=springboot&logoColor=white)
![Raft](https://img.shields.io/badge/consensus-Raft-ffc94d)
![Protocol](https://img.shields.io/badge/protocol-memcached%20text-blue)
[![Live demo](https://img.shields.io/badge/demo-live-brightgreen)](https://beedb.subodhlatkar.com)

[Live demo](https://beedb.subodhlatkar.com) · [Features](#features) · [Quickstart](#quickstart) · [How it works](#how-it-works) · [Numbers](#numbers) · [The 478 MB WAL](#the-478-mb-wal) · [Limitations](#limitations) · [Blog series](https://subodhlatkar.com/blogs/)

</div>

---

## What it is

BeeDB is a three-node replicated key-value store. The consensus algorithm, the write-ahead log,
the peer transport and the memcached protocol parser are all in this repository. It does not use
Netty, jRaft or any consensus library.

A `set` is appended to the Raft log, fsync'd to a CRC-framed write-ahead log, replicated to a
majority of nodes, and only then acknowledged with `STORED`. If the leader dies, the other two
elect a new one and keep serving.

Because it speaks the memcached text protocol, any memcached client, or plain `telnet`, works:

```
$ telnet localhost 11211
set greeting 0 0 5
hello
STORED
get greeting
VALUE greeting 0 5
hello
END
```

It started as John Crickett's [build your own memcached](https://codingchallenges.fyi/challenges/challenge-memcached/)
coding challenge and grew from there.

**Why "BeeDB"?** A hive has one queen, the workers follow her, and when she dies the colony
raises a new one. That is Raft: one leader, followers that replicate its decisions, and an
election when it is gone.

---

## Features

**Storage and protocol**
- memcached text protocol on TCP: `set`, `add`, `replace`, `append`, `prepend`, `get`, `delete`, `stats`
- Per-key flags and expiry times, with expired keys hidden from reads
- `stats` reports each node's Raft role, term, commit index and applied index

**Consensus (Raft, written from scratch)**
- Leader election with randomized election timeouts and heartbeats
- Log replication through `AppendEntries`, with one replicator thread per follower
- The leader appends a no-op entry when it takes over, so entries from earlier terms get committed
- Followers use Raft's per-entry conflict check (§5.3): a resent batch writes nothing new, and a real
  conflict truncates the log from that index
- A follower never acknowledges entries that are not yet fsync'd, including on a retry after a slow disk
- The leader never commits past its own fsync'd index (`min(majorityMatchIndex, persistedWalIndex)`)
- `currentTerm` and `votedFor` are saved to disk, so a restarted node cannot vote twice in one term
- Snapshots, log compaction and `InstallSnapshot` for followers that fall too far behind
- Only the leader accepts writes. Other nodes reply `Not Leader: <id>`, or `Election in progress`
  when there is no leader yet

**Durability**
- Write-ahead log with a CRC on every record. A torn or corrupt tail is truncated on replay
- Group commit: one WAL thread batches concurrent writes into a single fsync
- The directory is fsync'd after an atomic rename, so the rename itself survives a crash
- Fail-stop: if a WAL write fails or the apply loop dies, the node halts instead of running on
  with state it cannot trust

**HTTP gateway (Spring Boot)**
- REST API for keys: `GET`, `PUT`, `DELETE`
- Finds the current leader by itself and keeps a connection pool to each node
- Returns `503` with `Retry-After` during an election, and `404` only when a key really is missing
- Live cluster state over Server-Sent Events
- Per-IP write limit, value size cap and TTL on every key, for running it in public
- Chaos mode: kills a node on a schedule and restarts it, so you can watch the cluster recover

**Operations**
- One-command local cluster with `make up`
- systemd units, a rolling deploy that waits for followers to catch up, and one-command rollback

---

## Quickstart

**Requirements:** Java 21 and Maven.

```bash
git clone https://github.com/codecasualty/beedb.git
cd beedb
mvn -q clean package -DskipTests
```

Start a local three-node cluster:

```bash
cd beedb-server
make up        # starts node1, node2, node3 on ports 11211, 11212, 11213
make status    # which nodes are running
```

Talk to it:

```bash
telnet localhost 11211      # or: nc localhost 11211
stats                       # look for "STAT raft_role LEADER"
```

Send writes to the leader. If this node is a follower it replies `Not Leader: <id>`, so connect
to that node's port instead.

Then kill the leader and read your key from one of the survivors. It is still there.

| Command | What it does |
|---|---|
| `make up` | build and start all three nodes |
| `make status` | show which nodes are running |
| `make tail` | follow the logs, useful for watching an election |
| `make restart` | stop and start the cluster |
| `make down` | stop everything |
| `make logclean` | clear the log files |

Each node reads its own `node<N>.properties`, so a cluster is three config files.

### HTTP gateway

`beedb-gateway` is a Spring Boot app that puts a REST API in front of the cluster. It finds the
leader, pools connections, and maps cluster errors to HTTP status codes.

```bash
mvn -pl beedb-gateway spring-boot:run

curl -X PUT    localhost:8080/api/kv/greeting -d 'hello'   # 200 STORED
curl           localhost:8080/api/kv/greeting              # 200 hello
curl           localhost:8080/api/kv/nope                  # 404
curl -X DELETE localhost:8080/api/kv/greeting              # 200 DELETED
```

| Endpoint | Purpose |
|---|---|
| `GET /api/kv`, `GET /api/kv/{key}` | list keys, read one |
| `PUT /api/kv/{key}` | write a value |
| `DELETE /api/kv/{key}` | delete a key |
| `GET /api/cluster` | snapshot of every node: role, term, commit and applied index |
| `GET /api/events` | the same snapshots as a Server-Sent Events stream |
| `POST /api/kill` | chaos: schedule a node to be killed and restarted |
| `GET /api/quota` | remaining writes for the caller |

`404` means the key does not exist. `503` with `Retry-After: 1` means the cluster is
electing a new leader and the request can be retried in a second.

Settings live in `beedb-gateway/src/main/resources/application.properties`
(node addresses, key TTL, per-IP write limit, max value size).

---

## How it works

```
  memcached client ──────────────┐
  (telnet, nc, any client lib)   │ memcached text protocol
                                 ▼
  browser ──► gateway ──────► ┌────────┐  AppendEntries  ┌──────────┐
  (REST/SSE)  (Spring Boot)   │ LEADER │────────────────►│ FOLLOWER │
                              └───┬────┘────────┐        └──────────┘
                                  │ fsync       │        ┌──────────┐
                               ┌──▼──┐          └───────►│ FOLLOWER │
                               │ WAL │                   └──────────┘
                               └─────┘
```

**The path of one write:**

1. A connection thread parses the memcached command.
2. The leader appends it to the Raft log and hands it to the WAL writer.
3. The WAL thread batches concurrent writes and fsyncs them together (about 11 records per fsync).
4. At the same time, `AppendEntries` RPCs carry the entry to the followers.
5. The entry commits once a majority has it **and** the leader's own fsync has finished. Then the
   client gets `STORED`.

Step 5 is stricter than the textbook rule. If the leader counted only the followers' copies, it
could acknowledge a write it had not yet persisted, crash, come back without it, and win the next
election. So the leader never commits past its own durable point:

```java
commitIndex = min(majorityMatchIndex, persistedWalIndex);
```

That cost about 19% of throughput (1,929 to 1,560 writes/sec). The faster number came from a
version that could lose acknowledged writes.

**Durability details**

- Every WAL record carries a CRC, so a torn tail left by a crash is detected and truncated on replay.
- Concurrent writes are group-committed: drained from a queue and fsync'd together.
- After an atomic rename, the directory is fsync'd too, so the rename itself is durable.
- Snapshots and log compaction keep the log from growing forever.

`DurabilityIT` crashes the leader during traffic and also restarts the whole cluster. No
acknowledged write was lost in either case.

---

## Numbers

Three nodes on one laptop, loopback network, 32-byte values, consumer NVMe.

| Metric | Value |
|---|---|
| Write throughput, 64 concurrent clients | 1,560 writes/sec, 0 errors |
| Through the HTTP gateway, 64 threads, 30 s | 1,408 writes/sec, 0 errors |
| Write latency | p50 6.2 ms, p95 8.7 ms, p99 12.2 ms |
| Read latency | p50 0.037 ms (reads skip consensus, see [Limitations](#limitations)) |
| WAL group commit | 11 records per fsync |
| Acknowledged writes lost, leader crash and full restart | 0 |
| Failover through the gateway | 979 of 1,000 acked, 0 lost, new leader found in about 1 s |
| Memory, measured on an Ubuntu 24.04 VM | about 65 MB RSS per node, 190 MB for the gateway |

On the live server (3 nodes on one 2-vCPU VPS in Singapore, loopback, 32-byte values, 8 clients):
1,567 writes/sec, write p50 4.2 ms and p99 20.4 ms, read p50 0.073 ms.

Throughput over the project: 125, 362, 369, 1,929, then 1,560 writes/sec. The last drop is the
commit-rule fix above.

---

## The 478 MB WAL

After 43 hours of chaos on the live demo, one follower's write-ahead log had grown to 478 MB. It
held the same five entries, written about 500,000 times each. Replaying it ran out of memory, so
the node crashed on every restart, and a second node had started doing the same.

Two bugs together caused it:

- **The follower re-journaled every resent batch.** It truncated whenever the request overlapped
  its log, instead of only when an entry's term actually differed. So every resend wrote a
  TRUNCATE record and all the entries again.
- **The leader resent immediately.** When a response didn't move the follower forward, the
  replication loop sent the same batch again with no pause, about 4,400 times a second.

The fix is Raft's per-entry conflict check (append only what is new), a barrier so a retry is
never acknowledged before its entries are on disk, and a short pause before resending. The
same change fixed an election bug where a node could lead a term it had just voted in for
someone else. After the fix the demo ran 37 hours under the same chaos: the WAL never went
above 186 KB and the commit index moved forward in every 10-minute sample.

---

## Limitations

- **Reads can be stale.** Any node answers reads from its local state, with no leader check.
  A leader cut off from the others will still answer. The fix is ReadIndex (Raft thesis §6.4).
- **Writes are at-least-once.** A write that times out may still land, so a retry can apply it
  twice. The fix is client sessions with request dedup (§6.3).
- For that reason `APPEND`, `PREPEND`, `ADD` and `REPLACE` exist in the server but are not exposed
  over HTTP. They are not safe to retry.
- **Fixed membership.** The cluster is three nodes. No membership changes and no PreVote (§9.6).
- **Values are capped at 1,400 bytes** by the HTTP gateway. An early version of the server expected
  each command to arrive in one network read, and a bigger value crashed the node. 1,400 bytes fits
  in a single TCP packet (about 1,460 bytes of data), so in practice it arrived in one read. The server
  now reads commands in pieces and accepts up to 8 KB, but the demo keeps the smaller cap.
- **The server doesn't check a value's size before reading it.** A client can announce a huge value,
  send a few bytes and stop, and the connection stays open. This is safe today only because the
  nodes' ports are firewalled and only the gateway can reach them.
- `RaftClusterTest.shouldReadWalAfterRestart` is flaky, failing about 1 run in 3.

---

## Repository layout

```
beedb-server/     the database
  raft/           RaftNode, RaftLog, elections, replication, snapshots
  raft/rpc/       peer transport (AppendEntries, RequestVote)
  raft/wal/       CRC-framed write-ahead log, group commit, recovery
  command/, handler/, response/   memcached text protocol
  cache/          the in-memory key-value state
  Makefile        local three-node cluster (make up / status / down)
beedb-gateway/    Spring Boot REST + SSE front end
  client/         BeedbConnection (one socket), ConnectionPool, BeedbClient (leader lookup)
  api/            REST controllers, error-to-status mapping
  cluster/        node status snapshots
  chaos/          scheduled node kills for the demo
```

### Tests

```bash
mvn verify
```

| Suite | Tests | What it covers |
|---|---|---|
| `RaftClusterTest` | 19 | elections, replication, snapshots on an in-memory transport |
| `WalServiceTest` | 5 | WAL append, read, recovery from corrupted records |
| `SocketClusterIT` | 3 | the same cluster over real sockets |
| `DurabilityIT` | 2 | leader crash and full-cluster restart with no lost writes |
| `ChaosServiceTest` | 18 | the gateway's kill-and-restart state machine |
| `ServerRoutingTest` | 1 | SET and DELETE go through the Raft log; GET and STATS are answered locally |
| `AppendEntriesVerifyTest` * | 10 | resends, conflicts, stale tails, WAL timeouts; every case also restarts the node and checks WAL replay |
| `ExpiryTest` * | 9 | expiry is fixed once on the leader, so replay and followers agree |

\* written with Claude, to verify fixes I made. Everything else in the table is mine.

The socket suite exists because an in-memory transport cannot fail the way a socket does. It
found three bugs on its first run that every in-memory test had missed.

---

## Deploying

The live demo runs on a single Ubuntu 24.04 VM as four systemd units: three nodes and the
gateway. The deploy scripts are not in this repository, but the approach is simple to copy.

Each release goes to `/opt/beedb/releases/<git-sha>` and a `current` symlink is swapped
atomically. The rolling restart does followers first and the leader last, and waits for each
node to catch up (`last_applied >= commit_index`) before moving on. nginx sits in front of the
gateway, and the server only accepts traffic from Cloudflare.

The live demo at **[beedb.subodhlatkar.com](https://beedb.subodhlatkar.com)** runs this way on
a 2 vCPU / 4 GB VPS, with chaos mode killing nodes every few minutes.

---

## How this was built

The Raft implementation, the write-ahead log, the server and the gateway are my code. I used
Claude as a reviewer while building them, and it wrote the demo frontend, some of the
verification tests marked above, and helped draft the blog posts from my notes.

---

## License

MIT. Free to use, modify and distribute. See [LICENSE](LICENSE).

---

<div align="center">

Built by [Subodh Latkar](https://subodhlatkar.com) · [GitHub](https://github.com/codecasualty)

</div>
