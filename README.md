# RaftKV: a distributed key-value store built on Raft

RaftKV is a from-scratch implementation of the **Raft consensus algorithm** in Java. A
replicated, fault-tolerant **key-value store** runs on top of it. Five nodes talk to each
other over **gRPC**, persist their Raft state in **PostgreSQL**, and expose a **REST API**
built with **Spring Boot**.

The cluster keeps working, with no lost data, while any 2 of its 5 nodes are down.

> **New to Raft?** Start with [docs/LEARNING_GUIDE.md](docs/LEARNING_GUIDE.md). It
> explains consensus from first principles and walks through this codebase.

---

## Features

| Raft feature | Where |
|---|---|
| Leader election with randomized timeouts and majority voting | `RaftNode.startElection`, `handleRequestVote` |
| Election restriction: only up-to-date logs can win | `RaftNode.candidateLogIsUpToDate` |
| Log replication with the AppendEntries consistency check | `RaftNode.sendAppendEntries`, `handleAppendEntries` |
| Commit by majority quorum, current-term-only rule (Figure 8) | `RaftNode.maybeAdvanceCommitIndex` |
| No-op entry on election, to commit leftover entries | `RaftNode.becomeLeader` |
| Fast log backtracking (conflict term/index hints) | `handleAppendEntriesResponse` |
| Durable term, vote, and log, written before replying | `PostgresRaftStorage` |
| Linearizable reads, by sending GETs through the log | `KvController.get` |
| Follower → leader redirects (HTTP 307) | `ApiExceptionHandler` |
| Network-partition simulation over REST | `RaftController`, `NetworkSimulator` |

## Architecture

```
                   Client (curl / app)
                          │  REST: PUT/GET/DELETE /kv/{key}
                          ▼
 ┌─────────────────────── one node (one JVM process) ───────────────────────┐
 │  KvController ──submit(cmd)──► RaftNode ──apply(cmd)──► KeyValueStateMachine│
 │       ▲                         │   ▲                      (HashMap)      │
 │       └── result when committed ┘   │                                     │
 │                                     │                                     │
 │          RaftStorage ◄──────────────┤ term, votedFor, log[]               │
 │      (PostgreSQL / memory)          │                                     │
 │                                     ▼                                     │
 │                 GrpcRaftTransport (client)   RaftGrpcService (server)     │
 └──────────────────────────────────┬───────────────────▲────────────────────┘
                                    │  gRPC: RequestVote, AppendEntries
                                    ▼                   │
                         the 4 other nodes (same code, different NODE_ID)
```

How a write flows:
1. The client sends `PUT /kv/color` to any node. A follower answers `307` → leader.
2. The leader appends `{PUT color blue}` to its log and sends `AppendEntries` to all followers in parallel.
3. Each follower checks the entry fits its log, **persists it**, and replies success.
4. Once a **majority (3 of 5)** have stored it, the leader marks it committed.
5. The leader applies it to its HashMap and replies to the client. Followers apply it
   once the next heartbeat tells them the new commit index.

## Quick start

**Requirements:** Java 21+. Maven isn't needed; use the included `mvnw` wrapper.

### Option A: 5 nodes on your machine (in-memory storage)

```powershell
# Windows (PowerShell)
.\mvnw.cmd package
.\scripts\start-cluster.ps1
```
```bash
# macOS / Linux
./mvnw package
./scripts/start-cluster.sh
```

Nodes listen on `http://localhost:8081` … `8085` (gRPC on `9091` … `9095`). Stop them
with `scripts/stop-cluster.(ps1|sh)`.

### Option B: Docker Compose (PostgreSQL persistence)

```bash
docker compose up --build
```

This starts PostgreSQL plus 5 nodes. Each node has its own database (`raft_n1` …
`raft_n5`), so stopping and restarting a container shows real crash recovery.

### Run the tests

```bash
./mvnw test
```

The tests spin up a 5-node cluster inside one JVM over a simulated network. They check
leader election, replication, leader crash, losing 2 vs. 3 nodes, minority/majority
partitions, restart from disk, and log matching after repeated leader changes.

## API

> On Windows PowerShell, type `curl.exe` instead of `curl`.

### Key-value

```bash
# Write (send to any node; -L follows the redirect to the leader)
curl -L -X PUT localhost:8083/kv/name -H 'Content-Type: text/plain' -d 'raft'
# {"key":"name","value":"raft","found":false,"index":2}

# Linearizable read (goes through the Raft log)
curl -L localhost:8085/kv/name

# Stale read (served locally by any node, may lag behind)
curl "localhost:8084/kv/name?consistency=stale"

# Delete
curl -L -X DELETE localhost:8082/kv/name

# All keys held by one node (local view)
curl localhost:8085/kv
```

| Status | Meaning |
|---|---|
| `200` | Committed and applied |
| `307` | This node is a follower; `Location` points to the leader |
| `404` | Key not found |
| `503` | No leader yet (election in progress) or leadership changed mid-request; safe to retry |
| `504` | Not committed within 5 s (no majority reachable); it *may* still commit later |

### Inspecting Raft

```bash
curl localhost:8081/raft/status           # role, term, votedFor, commitIndex, nextIndex/matchIndex...
curl "localhost:8081/raft/log?from=1&limit=20"
curl -X POST localhost:8083/raft/admin/isolate   # cut node off from the cluster
curl -X POST localhost:8083/raft/admin/heal      # reconnect it
```

## Try breaking it

| Experiment | What you should see |
|---|---|
| Kill the leader process | Within about 1 s a follower times out, wins with 3+ votes, and the term increases. Old data is still there. |
| Kill 2 nodes | Writes still succeed (3 of 5 is a majority). |
| Kill 3 nodes | Writes hang and return `504`. Restart one and they commit again. |
| `isolate` a follower | Its term climbs as it keeps starting elections it can't win. On `heal`, its high term forces a re-election. That's why production Raft adds **Pre-Vote**. |
| `isolate` the leader | It still thinks it leads but can't commit. The majority elects a new leader. On heal, the old leader steps down and its uncommitted entries are overwritten. |

More guided experiments are in [docs/LEARNING_GUIDE.md](docs/LEARNING_GUIDE.md#9-hands-on-experiments).

## Project structure

```
src/main/proto/raft.proto            gRPC contract: RequestVote + AppendEntries
src/main/java/com/raftkv/
├── raft/                            The consensus engine (no Spring dependencies)
│   ├── RaftNode.java                ★ election, replication, commit, apply
│   ├── RaftLog.java                 1-based log, mirrored to storage
│   ├── LogEntry.java, Role.java, StateMachine.java, NodeStatus.java
│   ├── storage/                     RaftStorage, PostgresRaftStorage, InMemoryRaftStorage
│   └── transport/                   RaftTransport, GrpcRaftTransport, RaftGrpcService, NetworkSimulator
├── kv/                              The replicated application
│   ├── KeyValueStateMachine.java    deterministic HashMap state machine
│   └── Command.java, KvResult.java
├── api/                             REST: KvController, RaftController, ApiExceptionHandler
└── config/                          Spring wiring, gRPC server lifecycle, properties
src/test/java/com/raftkv/raft/       LocalCluster (simulated network) + cluster tests
docker-compose.yml, Dockerfile       5 nodes + PostgreSQL
docs/                                Learning guide
```

## Configuration

`src/main/resources/application.yml` (overridable via env vars):

| Property | Default | Notes |
|---|---|---|
| `raft.node-id` / `NODE_ID` | `n1` | Which cluster member this process is |
| `server.port` / `HTTP_PORT` | `8081` | REST port |
| `raft.grpc-port` / `GRPC_PORT` | `9091` | Raft RPC port |
| `raft.heartbeat-interval` | `100ms` | Leader heartbeat period |
| `raft.election-timeout-min/max` | `500ms` / `1000ms` | Randomized per election |
| `raft.storage.type` / `RAFT_STORAGE` | `memory` | `memory` or `postgres` |
| `raft.storage.url` / `RAFT_DB_URL` | `jdbc:postgresql://localhost:5432/raftkv` | |

## Known limitations

This is a learning implementation. Production systems like etcd add the features below,
all explained in the learning guide:

- No **log compaction / snapshots**: the log grows forever, and a restart replays it all.
- No **cluster membership changes**: the 5 members are fixed in config.
- No **Pre-Vote** or **CheckQuorum**: a rejoining partitioned node can trigger a needless election.
- No **client request deduplication**: retrying a timed-out PUT could apply it twice.
- Linearizable reads go through the log (simple but slow) rather than **ReadIndex** or leader leases.

## References

- Diego Ongaro & John Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) (the Raft paper). Figure 2 is the spec this code follows.
- [raft.github.io](https://raft.github.io/): an interactive visualization (highly recommended)
- Diego Ongaro, [*Consensus: Bridging Theory and Practice*](https://github.com/ongardie/dissertation) (PhD thesis: membership changes, Pre-Vote, ReadIndex)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/), labs 3A–3D
