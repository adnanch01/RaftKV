# RaftKV Learning Guide

This guide teaches distributed consensus and the Raft algorithm from first principles,
then maps every idea onto the code in this repository. Read it top to bottom once. After
that, use it as a reference.

**Contents**
1. [The problem: why consensus?](#1-the-problem-why-consensus)
2. [Raft in one page](#2-raft-in-one-page)
3. [Leader election](#3-leader-election)
4. [Log replication](#4-log-replication)
5. [Safety: why Raft never loses committed data](#5-safety-why-raft-never-loses-committed-data)
6. [Persistence and crash recovery](#6-persistence-and-crash-recovery)
7. [Clients: redirects, reads, and retries](#7-clients-redirects-reads-and-retries)
8. [Code walkthrough](#8-code-walkthrough)
9. [Hands-on experiments](#9-hands-on-experiments)
10. [Beyond this project: what production Raft adds](#10-beyond-this-project-what-production-raft-adds)
11. [Raft in context: Paxos, ZAB, and real systems](#11-raft-in-context-paxos-zab-and-real-systems)
12. [Glossary](#12-glossary)

---

## 1. The problem: why consensus?

### One server is a single point of failure
A key-value store on one machine is simple: every read sees the latest write. But when
that machine dies, the service is down, and if the disk dies, the data is gone.

### Copying data to several servers creates a new problem
Run 5 copies instead. Now every copy must agree on the same data, in the same order,
even when:
- servers **crash** and restart at any moment,
- the network **delays, drops, duplicates, or reorders** messages,
- the network **partitions**, splitting the servers into groups that can't talk to each other,
- **clocks are not synchronized**, so you can't order events by timestamp.

Naive approaches fail. Picture two clients writing `x=1` and `x=2` to different replicas
at the same moment. Without a protocol, replica A may end with `x=1` and replica B with
`x=2`, and they never converge. This is the **consensus** problem: getting several
machines to agree on one value, or here, a sequence of values.

### The Replicated State Machine idea
The standard solution, and what Raft provides:

> If every server starts in the same state and applies the **same commands in the same
> order**, and the commands are **deterministic**, every server ends in the same state.

So the problem shrinks to agreeing on an ordered **log** of commands. Raft's whole job is
keeping that log identical across servers. The application on top (here, a HashMap) is
called the **state machine**. It just applies log entries in order.

```
 client ──► [ Consensus module ]  ──►  log: [set x=1][set y=2][del x] ...  ──►  [ State machine ]
             (Raft: RaftNode)           (identical on every server)              (HashMap)
```

In this repo: `RaftNode` is the consensus module, `RaftLog` is the log, and
`KeyValueStateMachine` is the state machine. `KeyValueStateMachine` doesn't know Raft exists.

### What "fault tolerant" means precisely
Raft is safe as long as no server is malicious. That's the **crash-fault** model, as
opposed to the **Byzantine** model with lying servers, where you'd need PBFT or similar.
Raft makes progress (commits new entries) whenever a **majority** of servers is up and can
talk to each other.

| Cluster size N | Majority (quorum) | Failures tolerated |
|---|---|---|
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 4 | 3 | 1 ← no better than 3! |
| **5** | **3** | **2** ← this project |
| 7 | 4 | 3 |

General rule: **N = 2f + 1 servers tolerate f failures.** Even sizes add cost without
adding tolerance, which is why clusters are almost always 3 or 5 nodes. Larger clusters
make every write wait for more acknowledgements.

**Why majorities work:** any two majorities of the same cluster **overlap in at least one
server**. With 5 nodes, two groups of 3 must share a member. That shared member carries
knowledge from one decision to the next. It's the root of every safety argument below.

### A note on theory
- **FLP impossibility (1985):** in a fully asynchronous network where even one process
  can crash, no deterministic algorithm can guarantee consensus terminates. Raft does
  not contradict this. It is **always safe**, but only **live** (makes progress) when the
  network behaves well enough for an election to finish. Randomized timeouts make that
  overwhelmingly likely in practice.
- **CAP theorem:** during a partition you must choose between consistency and
  availability. Raft chooses **consistency (CP)**: the minority side refuses writes rather
  than accept data that might conflict.

---

## 2. Raft in one page

Raft (Ongaro & Ousterhout, 2014) was designed to be **understandable**, unlike Paxos. It
splits consensus into three mostly independent subproblems:

1. **Leader election:** pick one server to be in charge.
2. **Log replication:** the leader accepts commands and copies its log to everyone else.
3. **Safety:** rules that guarantee a committed entry is never lost or changed.

### Server roles
Every server is in one of three states (`Role.java`):

- **Follower:** passive. Answers RPCs and expects regular heartbeats from a leader.
- **Candidate:** a follower that heard nothing for a while and is asking for votes.
- **Leader:** handles all client writes and replicates them. At most one per term.

```
          timeout                 majority of votes
FOLLOWER ─────────► CANDIDATE ──────────────────────► LEADER
   ▲  ▲                │  │ timeout: retry election     │
   │  └────────────────┘  └──► CANDIDATE                │
   │   sees current leader                              │
   │   or a higher term                                 │
   └────────────────── sees a higher term ──────────────┘
```

### Terms: Raft's logical clock
Time is divided into **terms** numbered 1, 2, 3, … Each term starts with an election.
If someone wins, they lead for the rest of the term. If the vote splits, the term ends
with no leader and a new one starts.

```
term:   |   1   |   2   | 3 |     4     |
        |elect|normal operation |elect (split vote, no leader)|elect|normal...
```

Terms let servers detect stale information without synchronized clocks:
- Every RPC carries the sender's term.
- **If you see a higher term, adopt it and become a follower.** (`becomeFollower`)
- **If you get a request with a lower term, reject it.** It's from a deposed leader.

### Only two RPCs
| RPC | Sent by | Purpose |
|---|---|---|
| **RequestVote** | Candidates | "Vote for me in term T. My log ends at (index, term)." |
| **AppendEntries** | Leader | "Here are new entries after (prevIndex, prevTerm). Also, commit up to X." An empty one is a **heartbeat**. |

They're defined in `src/main/proto/raft.proto` and implemented in `RaftNode`.

### Figure 2: the whole algorithm on one page
The Raft paper's Figure 2 is a precise spec of every rule. This code follows it closely,
and comments like `(Fig. 2)` mark where. Print Figure 2 and keep it next to
`RaftNode.java` while you read.

---

## 3. Leader election

### The election timeout
Every follower has an **election timer**. Each valid heartbeat from the leader resets it.
If it expires, the follower assumes the leader is dead and starts an election.

**Code:** `RaftNode.tick()` runs every 10 ms and checks whether
`now >= electionDeadlineNanos`. `resetElectionDeadline()` picks a new deadline.

### Starting an election (`startElection`)
1. Increment `currentTerm`.
2. Become **candidate** and **vote for yourself**.
3. **Persist** term and vote to disk.
4. Reset the election timer to a new random value.
5. Send `RequestVote(term, candidateId, lastLogIndex, lastLogTerm)` to every peer in parallel.

Three outcomes:
- **Win:** votes from a majority (3 of 5, counting yourself) → become leader and send heartbeats right away.
- **Lose:** an `AppendEntries` arrives from a leader with term ≥ yours → become follower.
- **Split vote:** nobody gets a majority → the timer expires again, so retry in a new term.

### Voting rules (`handleRequestVote`)
A server grants its vote only if **all** of these hold:
1. The candidate's term is ≥ its own. (A higher term makes it adopt that term first.)
2. It hasn't voted for anyone else **in this term**. One vote per term, persisted to disk.
3. The candidate's log is **at least as up to date** as its own (the *election restriction*, §5).

Rule 2 is why at most one leader exists per term (**Election Safety**). Each server casts
one vote per term, and two candidates can't both collect a majority from 5 votes.

### Why randomized timeouts? (liveness)
Suppose all 5 followers used exactly 500 ms. When the leader dies, all 5 time out
together, each votes for itself, and nobody wins. Then they all retry together again,
possibly forever.

Raft picks a **random** timeout per election, here between 500 and 1000 ms
(`raft.election-timeout-min/max`). Usually one server times out first, collects votes
before anyone else wakes up, and starts sending heartbeats that stop the others'
elections. Split votes still happen occasionally, but repeated splits become
exponentially unlikely.

### The timing requirement
```
broadcastTime  <<  electionTimeout  <<  MTBF (mean time between failures)
   (~1-10 ms)        (500-1000 ms)            (months)
```
- The heartbeat interval (100 ms) must be much smaller than the election timeout.
  Otherwise followers time out between heartbeats and hold pointless elections.
- The election timeout must be much smaller than the time between failures, or the
  system spends a big fraction of its time with no leader.

### Election walkthrough (what you actually see in the logs)
```
n1 leader in term 4 crashes
  t=0      n2,n3,n4,n5 stop getting heartbeats
  t=640ms  n4's timer fires first ─► term 5, votes for itself, RequestVote → all
  t=642ms  n2,n3,n5: term 5 > 4 → adopt term 5; n4's log is up to date → grant
  t=643ms  n4 has 4 votes ≥ 3 → LEADER for term 5; sends heartbeats
  t=650ms  n2,n3,n5 reset their timers. Election over.
```

---

## 4. Log replication

### Log structure
Each log entry has an **index** (position, starting at 1), a **term** (when the leader
created it), and a **command**.

```
index:   1      2      3      4      5
term:  [ 1 ] [ 1 ] [ 2 ] [ 3 ] [ 3 ]
cmd:    x=1   y=2   x=5  del y  z=0
                          ▲
                    commitIndex = 4 (entries 1..4 are on a majority)
```

`RaftLog.java` keeps a sentinel at index 0 with term 0, so the first real entry has a
"previous entry" too and needs no special case.

### The normal case (a client write)
1. The client sends a command to the leader.
2. The leader appends it to its own log (persisted).
3. The leader sends `AppendEntries` to all followers in parallel.
4. Followers persist the entry and reply success.
5. Once a **majority** has the entry, it is **committed**. The leader advances `commitIndex`.
6. The leader applies the entry to its state machine and replies to the client.
7. The next AppendEntries (or heartbeat) carries `leaderCommit`. Followers advance their
   own `commitIndex` and apply too.

**Commit** means the entry is durable and will never be lost, even if the leader dies
right now. **Apply** means the state machine executed it. Applying always happens after
committing, and always in index order.

### The consistency check: how logs stay identical
Each AppendEntries includes **`prevLogIndex` and `prevLogTerm`**: the index and term of
the entry just before the new ones. The follower **rejects** the request unless its own
log has an entry at `prevLogIndex` with term `prevLogTerm`.

That simple check gives the **Log Matching Property**:
> If two logs have an entry with the same index and term, then (a) they hold the same
> command, and (b) the logs are identical in all entries up to that point.

(a) holds because a leader creates at most one entry per index in a term, and entries
never move. (b) follows by induction: each append was only accepted after checking the
entry before it matched.

### Repairing divergent logs
After crashes and leader changes, a follower's log can differ from the leader's. It can
be missing entries, or hold extra *uncommitted* entries from an old leader:

```
index:       1  2  3  4  5  6
leader (t5): 1  1  2  3  3  5
follower A:  1  1  2                 ← missing entries
follower B:  1  1  2  4  4           ← extra uncommitted entries from a term-4 leader
```

**The leader's log is always the truth.** For each follower the leader tracks:
- **`nextIndex[peer]`:** the next entry to send. It starts optimistic, at leader's last index + 1.
- **`matchIndex[peer]`:** the highest entry known to match. It starts pessimistic, at 0.

When the consistency check fails, the leader moves `nextIndex` back and retries. It
eventually reaches the last entry where the logs agree. The follower then **deletes its
conflicting suffix** and appends the leader's entries.

**Code:** the follower side lives in `handleAppendEntries`, steps 2–4. The leader side
is in `handleAppendEntriesResponse`.

### Optimization: fast backup
Going back one entry per round trip is slow if a follower is far behind. In this
project, the follower's rejection includes hints:
- `conflictTerm`: the term of its conflicting entry at `prevLogIndex`
- `conflictIndex`: the first index where that term begins in its log

The leader then skips the whole conflicting term in one step: to its own last entry of
`conflictTerm` + 1 if it has one, otherwise to `conflictIndex`. A too-short follower log
reports `conflictTerm = 0` and `conflictIndex = its length + 1`. This is the optimization
the MIT 6.824 course teaches.

### Subtle bug to avoid: never truncate blindly
A follower must delete entries **only where they actually conflict** (same index,
different term). Picture an old, delayed AppendEntries arriving after a newer one.
Truncating everything after `prevLogIndex` would delete entries the newer RPC added,
and some of those may already be committed. See the `continue` in `handleAppendEntries`:
matching entries are skipped, not rewritten.

---

## 5. Safety: why Raft never loses committed data

Raft guarantees five properties (Figure 3 of the paper):

| Property | Meaning | Enforced by |
|---|---|---|
| **Election Safety** | At most one leader per term | One persisted vote per term + majority |
| **Leader Append-Only** | A leader never overwrites or deletes its own entries | Leaders only append (`appendToOwnLog`) |
| **Log Matching** | Same index and term → identical logs up to there | prevLogIndex/prevLogTerm check |
| **Leader Completeness** | A committed entry appears in every future leader's log | Election restriction + commit rule |
| **State Machine Safety** | No two servers apply different commands at the same index | All of the above |

The two less obvious rules are below.

### Rule 1: the election restriction (§5.4.1)
A voter rejects any candidate whose log is **less up to date** than its own:
- compare the **last entry's term** first: higher wins;
- if the last terms are equal, the **longer log** wins.

**Why it works:** a committed entry is stored on a majority. A winning candidate needs
votes from a majority. Two majorities overlap, so at least one voter holds the committed
entry, and that voter refuses any candidate whose log lacks it. So **every new leader
already has every committed entry**. Data never needs to flow from followers to the
leader. That's a big simplification over Paxos.

**Code:** `candidateLogIsUpToDate()`.

### Rule 2: only commit entries from your own term (§5.4.2, "Figure 8")
This is the most subtle part of Raft.

**The rule:** a leader advances `commitIndex` to N only if a majority has entry N
**and** `log[N].term == currentTerm`. Older-term entries become committed *indirectly*,
when a later current-term entry commits.

**Why?** Being stored on a majority isn't enough for entries from earlier terms.
Five servers, S1–S5:

```
(a) S1 leads term 2, writes entry @index2 (term 2), replicates it only to S2.
        S1: [1][2]   S2: [1][2]   S3: [1]   S4: [1]   S5: [1]
(b) S1 crashes. S5 wins term 3 (votes from S3, S4, S5), writes @index2 (term 3), crashes.
        S5: [1][3]
(c) S1 restarts, wins term 4, and keeps replicating its old term-2 entry to S3.
        S1: [1][2]   S2: [1][2]   S3: [1][2]     ← term-2 entry is on a MAJORITY (3/5)
    If S1 committed it now, based on counting replicas...
(d) ...S1 crashes. S5 can win term 5: its last term (3) beats everyone's last term (2),
    so S2, S3, S4 vote for it. S5 then overwrites index 2 with its term-3 entry on all
    servers. A "committed" entry was lost. SAFETY VIOLATION.
(e) With the rule: in (c), S1 first replicates a term-4 entry to a majority. S5 can
    no longer win (last term 3 < 4), so the term-2 entry under it is safe.
```

**Code:** `maybeAdvanceCommitIndex()` stops scanning as soon as it reaches an entry whose
term isn't `currentTerm`.

**The no-op entry.** Because of this rule, a new leader that holds old-term entries
can't commit them until a client sends a new write, and that might never happen. So a
new leader immediately appends an empty **no-op** entry from its own term (in
`becomeLeader()`). Once the no-op commits, everything before it commits too. It also
lets a fresh leader find out what's committed, which matters for reads.

---

## 6. Persistence and crash recovery

### What must survive a crash
Figure 2 lists exactly three persistent fields:

| Field | Why it must be durable |
|---|---|
| `currentTerm` | Forgetting it lets a server accept messages from stale leaders, or reuse an old term |
| `votedFor` | Forgetting it lets a server **vote twice in one term** → two leaders → split brain |
| `log[]` | Forgetting entries lets a "majority" that acknowledged an entry silently shrink → committed data lost |

**And it must be written *before* replying to the RPC.** If a server replied "vote
granted" and then crashed before saving, it could restart and grant a second vote.

**Code:** `persistHardState()` is called before `handleRequestVote` returns. The
`raftLog.append()` inside `handleAppendEntries` writes to storage before the method
returns success.

### What is rebuilt
`commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`, and the state machine itself
are **volatile**. After a restart, a node loads its term, vote, and log, then starts as a
follower with `commitIndex = 0`. The first heartbeat from the leader carries
`leaderCommit`, and the node re-applies its log from index 1 to rebuild the HashMap. With
large logs this replay is slow, which is why real systems use **snapshots** (§10).

### Storage in this project
`RaftStorage` is an interface with two implementations:
- **`PostgresRaftStorage`:** tables `raft_hard_state (node_id, current_term, voted_for)`
  and `raft_log (node_id, log_index, term, command)`. In Docker, each node gets its own
  database, like each real server owning its own disk.
- **`InMemoryRaftStorage`:** for tests and quick local runs. Not durable across process restarts.

> **Design note:** *"Why PostgreSQL rather than a local file?"* It gave me
> transactional, durable writes, plus easy inspection with SQL while learning. The
> trade-off is a network round trip per write. Production systems (etcd, CockroachDB)
> use a local **write-ahead log** with `fsync`, batching many entries per sync. Storage
> sits behind an interface, so swapping in a file-based WAL wouldn't touch the
> algorithm.

---

## 7. Clients: redirects, reads, and retries

### Finding the leader
Only the leader may accept writes. Every follower knows the current leader from the
`leaderId` field of AppendEntries. A follower that gets a write answers **HTTP 307**
with the leader's address (`ApiExceptionHandler.notLeader`). 307, unlike 302, requires
the client to repeat the same method and body. If no leader is known (mid-election),
it answers 503 and the client retries.

### Waiting for commit
`RaftNode.submit()` appends the command and returns a `CompletableFuture`. The REST
thread waits on it, with a 5 s timeout. The future completes in
`applyCommittedEntries()` once the entry is committed and applied. It fails with
`ProposalDroppedException` if the entry gets overwritten because this leader lost
leadership and a new leader put a different entry at that index. The node detects that
by comparing the entry's term with the term recorded at submit time.

### Linearizable reads
**Linearizability:** every operation appears to happen instantly at some point between
its request and its response. So a read always sees the latest completed write.

Why not read straight from the leader's HashMap? A leader that was **partitioned away**
may not know it was replaced. It could serve stale data while the majority side has
already accepted newer writes. (Try it: experiment 4.)

This project's solution is simple: **send reads through the log as well**
(`Command.get`). The read gets a log index. It can only commit if the leader is still
the real leader, confirmed by a majority, and when it is applied, it sees every write
before it. The cost: every read takes a replication round trip and a disk write.

Production alternatives:
- **ReadIndex:** the leader records its `commitIndex`, confirms leadership with one
  heartbeat round to a majority, waits until `lastApplied ≥ that index`, then reads
  locally. No disk write. etcd does this.
- **Leader leases:** after a successful heartbeat round, the leader assumes nobody else
  can be elected for (election timeout − clock drift) and serves reads locally with no
  round trip. Fastest, but it **depends on bounded clock drift**.
- **Stale reads** (`?consistency=stale`): any node reads locally. Fast, scales out,
  may be behind. Fine for some use cases.

### Exactly-once semantics (not implemented, but know it)
If a PUT times out, the client can't tell whether it committed. Retrying could apply
it twice. That's harmless for `PUT x=5` (idempotent) but wrong for `INCREMENT x`. The
standard fix (§6.3 of the thesis): each client gets an ID and numbers its requests. The
state machine remembers the last sequence number applied per client and ignores
duplicates.

---

## 8. Code walkthrough

### Package map
```
raft/       the algorithm. No Spring imports, so it's testable in plain JUnit.
  RaftNode        ★ all of Raft: ~680 lines, heavily commented
  RaftLog         the log (1-based, sentinel at 0), mirrored to storage
  storage/        RaftStorage interface + Postgres and in-memory implementations
  transport/      RaftTransport interface + gRPC client/server + NetworkSimulator
kv/         the state machine (HashMap) and the command format (JSON bytes)
api/        REST controllers and HTTP error mapping
config/     Spring wiring: builds a RaftNode from application.yml
```

**Design choice: dependency inversion.** `RaftNode` depends only on the interfaces
`RaftTransport`, `RaftStorage`, and `StateMachine`. That's why the tests can run 5 nodes
in one JVM, with a fake network that crashes and partitions them on demand
(`LocalCluster`), and no gRPC, Postgres, or Spring involved.

### Concurrency model
- **One `ReentrantLock` per node guards all Raft state.** Every event takes it: a timer
  tick, an incoming RPC, an RPC response, a client submit.
- **No network I/O happens while holding the lock.** The node builds the request under
  the lock, releases it, then sends asynchronously. Otherwise one slow peer could freeze
  the node, or two nodes calling each other could deadlock.
- **Responses re-check the world.** By the time a response arrives, the node might have
  moved to a new term or lost leadership. Every handler checks
  `role == LEADER && currentTerm == requestTerm` (or the candidate equivalent) and drops
  stale responses. This is the main defence against concurrent state-transition bugs.
- **A single ticker thread** (`tick()`, every 10 ms) drives heartbeats and election
  timeouts by comparing `System.nanoTime()` with deadlines. `nanoTime` is monotonic and
  never jumps when the wall clock changes.
- Disk writes do happen under the lock. That's simple and correct, and the main
  throughput limit. See §10 on batching.

*Why not `synchronized` everywhere?* A single explicit lock is the same idea, and the
lock/unlock pairs make critical sections visible. *Why not an event loop / actor?* That
would work too (etcd's raft library is a single-threaded state machine fed by a
channel). A single lock is closer to the paper and easier to follow.

### Trace: what happens on `PUT /kv/color` sent to a follower
1. **`KvController.put`** on follower n3 calls `node.submit(...)`.
2. **`RaftNode.submit`** sees `role != LEADER` and throws `NotLeaderException("n1")`.
3. **`ApiExceptionHandler.notLeader`** returns `307 Location: http://localhost:8081/kv/color`.
4. curl `-L` repeats the PUT on n1. `submit` now:
   - appends `LogEntry(index=7, term=4, {PUT color blue})` (written to storage),
   - registers a `PendingProposal(term=4, future)` at index 7,
   - calls `broadcastAppendEntries()`.
5. **`sendAppendEntries(peer)`** for each of 4 peers, under the lock: reads `nextIndex[peer]=7`,
   builds `AppendEntries(term=4, prevLogIndex=6, prevLogTerm=4, entries=[#7], leaderCommit=6)`,
   then releases the lock and calls `transport.appendEntries(...)` (async gRPC).
6. On each follower, **`RaftGrpcService.appendEntries`** → **`RaftNode.handleAppendEntries`**:
   term OK → reset election timer → log has an entry at 6 with term 4 ✓ → append #7
   (persisted) → reply `success, matchIndex=7`.
7. Back on the leader, **`handleAppendEntriesResponse`** sets `matchIndex[peer]=7` and calls
   **`maybeAdvanceCommitIndex`**. After the 2nd follower answers, entry 7 is on 3 of 5
   nodes and has term 4 = currentTerm, so `commitIndex=7`.
8. **`applyCommittedEntries`** → `KeyValueStateMachine.apply` → `map.put("color","blue")`
   → completes the pending future with a `KvResult`.
9. `KvController` returns `200 {"key":"color","value":"blue","found":false,"index":7}`.
10. The next heartbeat carries `leaderCommit=7`. Followers set `commitIndex=7` and apply too.

### How the tests simulate failure (`LocalCluster`)
- `SimTransport.deliver` runs the target node's handler on a thread pool. It throws
  (dropping the message) if either node is crashed or they're in different partition
  groups, checked separately for the request and the response.
- `crash(id)` stops the node but **keeps its `InMemoryRaftStorage`**, like a disk that
  survives a power loss. `restart(id)` builds a fresh `RaftNode` from that storage, which
  really tests recovery from persistent state.

| Test | What it proves |
|---|---|
| `electsExactlyOneLeader` | Election Safety |
| `replicatesWritesToEveryNode` | Basic replication |
| `followersRejectWritesAndPointToTheLeader` | Leader discovery |
| `survivesLeaderCrashWithoutLosingCommittedData` | Leader Completeness, failover |
| `toleratesTwoFailuresButNotThree` | Quorum math: 3/5 commits, 2/5 can't |
| `minorityPartitionCannotCommitAndHealsCorrectly` | Split brain avoided; uncommitted entries overwritten; client told |
| `restartedNodeRecoversFromPersistentStateAndCatchesUp` | Durability, catch-up |
| `logsMatchAcrossAllNodesAfterChurn` | Log Matching after repeated leader changes |

---

## 9. Hands-on experiments

Start the cluster (`scripts/start-cluster.ps1` or `docker compose up --build`). In a
second terminal, use these helpers:

```bash
# Bash: show every node's role and term
status() { for p in 8081 8082 8083 8084 8085; do curl -s localhost:$p/raft/status \
  | grep -o '"nodeId":"[^"]*","role":"[^"]*","currentTerm":[0-9]*'; done; }
```
```powershell
# PowerShell equivalent
function status { foreach ($i in 1..5) { $s=(curl.exe -s "http://localhost:808$i/raft/status" | ConvertFrom-Json).raft; "$($s.nodeId) $($s.role) term=$($s.currentTerm) leader=$($s.leaderId) commit=$($s.commitIndex)" } }
```

### Experiment 1: watch an election
Run `status`. One LEADER, four FOLLOWERs, all in the same term. In the leader's log
file you'll see `election timeout, becoming CANDIDATE` and then `became LEADER`. The
term may be higher than 1: nodes that boot before the others time out with nobody to
vote for them, and bump their term each time.

### Experiment 2: replication
```bash
curl -L -X PUT localhost:8081/kv/a -d 1 -H 'Content-Type: text/plain'
curl localhost:8084/kv           # stale view on n4: should contain a=1
curl localhost:8084/raft/log     # same entries, same terms, on every node
```

### Experiment 3: kill the leader
Stop the leader (`docker compose stop n1`, or kill its process). Within about 1 s,
`status` shows a new leader in a **higher term**. Reads of `a` still return `1`
(Leader Completeness). Start the old leader again: it rejoins as a follower and catches up.

### Experiment 4: partition the leader (split brain avoided)
```bash
curl -X POST localhost:808X/raft/admin/isolate     # X = current leader
```
- The old leader still says LEADER in an old term. It doesn't know it's cut off.
- The other four elect a new leader in a higher term.
- `curl -X PUT localhost:808X/kv/b -d stale` on the old leader hangs and returns 504. It
  can't reach a majority.
- `curl "localhost:808X/kv/a?consistency=stale"` on the old leader returns possibly
  outdated data. That's exactly why linearizable reads go through the log.
- `heal` it: it sees the higher term, steps down, its uncommitted entry is overwritten,
  and it converges.

### Experiment 5: isolate a follower (the disruptive-rejoin problem)
Isolate a follower for about 5 seconds and watch its term climb (it keeps timing out).
Heal it. Its high term makes the leader step down when they next exchange messages,
which forces a **pointless election**. The rejoined node can't win, because its log is
behind and the election restriction rejects it, but it still caused disruption. That's
the problem **Pre-Vote** solves (§10).

### Experiment 6: lose quorum
Stop 3 of 5 nodes. Writes return 504, because no majority means no commits. Restart one
and they work again. The cluster favours **consistency over availability** (CP).

### Experiment 7: durability (Docker only)
Write some keys, run `docker compose stop` (all nodes), then `docker compose start`.
Data comes back, because the log was in Postgres and it gets re-applied. Look at the
storage directly:
```bash
docker compose exec postgres psql -U raft -d raft_n1 -c "select * from raft_hard_state; select log_index, term, encode(command,'escape') from raft_log;"
```

### Experiment 8: turn on debug logging
Set `com.raftkv.raft: DEBUG` in `application.yml` to see every vote and every rejected
AppendEntries with its backtracking.

---

## 10. Beyond this project: what production Raft adds

These are the natural next steps beyond this implementation.

### Log compaction (snapshots)
The log grows without bound, and restart replays everything. The fix: periodically
**snapshot** the state machine (serialize the HashMap) at some index, save it, and drop
all log entries up to that index. A follower too far behind gets the snapshot through
an **InstallSnapshot** RPC. Changes needed here: `RaftLog` would track a
`snapshotIndex/snapshotTerm` offset instead of assuming the list starts at 1.

### Cluster membership changes
Switching straight from config C_old to C_new is unsafe. During the transition, a
majority of the old config and a majority of the new config could elect two leaders.
Solutions:
- **Joint consensus** (original paper): go through a C_old,new phase that needs majorities of *both*.
- **Single-server changes** (thesis): add or remove one server at a time. Any majority
  of N and any majority of N±1 always overlap, so it's safe. Most implementations do this.

### Pre-Vote
Before incrementing its term, a would-be candidate asks: "would you vote for me?"
Peers say yes only if they haven't heard from a leader recently *and* the log is up to
date. A partitioned node can't win a pre-vote, so it never inflates its term and
doesn't disrupt the cluster when it rejoins (experiment 5).

### CheckQuorum / leader stickiness
A leader steps down if it hasn't heard from a majority within an election timeout, so
a partitioned leader stops accepting writes quickly. Followers also ignore RequestVote
if they've heard from a live leader recently.

### Performance techniques
- **Batching:** put many client commands in one AppendEntries and one `fsync`.
- **Pipelining:** send the next AppendEntries before the previous one is acknowledged.
- **Parallel disk write:** the leader can write to its own disk while sending to followers.
- **ReadIndex / leases** for reads (§7).
- **Multi-Raft:** shard the keyspace into many independent Raft groups so writes scale
  out (CockroachDB, TiKV).

### Leadership transfer
A graceful handoff for maintenance. The leader brings a target fully up to date, then
tells it to start an election immediately (`TimeoutNow`).

---

## 11. Raft in context: Paxos, ZAB, and real systems

| | Raft | (Multi-)Paxos | ZAB (ZooKeeper) |
|---|---|---|---|
| Goal | Understandability | Theoretical minimalism | Primary-backup broadcast |
| Leader | Strong leader; log flows leader → followers only | Leader is an optimization; any proposer can propose | Strong leader |
| Log gaps | Never: contiguous log | Allowed: slots decided independently | Never |
| Leader election | Built in, restricted to up-to-date logs | Not specified by basic Paxos | Built in, with a sync phase |
| Equivalent safety? | Yes. Both solve the same problem; Raft is easier to implement correctly. |

**Where Raft runs in production:** etcd (and so Kubernetes' control plane), Consul,
CockroachDB, TiKV/TiDB, YugabyteDB, MongoDB's replication protocol (Raft-like), Kafka
KRaft (which replaced ZooKeeper), RabbitMQ quorum queues, HashiCorp Vault/Nomad.

---

## 12. Glossary

| Term | Meaning |
|---|---|
| **Quorum / majority** | More than half the cluster: ⌊N/2⌋ + 1 |
| **Term** | Monotonic logical clock; each term has at most one leader |
| **Heartbeat** | Empty AppendEntries that keeps followers from starting elections |
| **Election timeout** | How long a follower waits without hearing from a leader before campaigning |
| **Committed** | Stored on a majority (with the current-term rule); guaranteed never lost |
| **Applied** | Executed by the state machine |
| **commitIndex** | Highest log index known committed |
| **lastApplied** | Highest log index applied to the state machine |
| **nextIndex[i]** | Leader's guess of the next entry to send follower i |
| **matchIndex[i]** | Highest entry the leader knows is replicated on follower i |
| **Split brain** | Two nodes both acting as leader and accepting conflicting writes. Raft prevents it by requiring a majority. |
| **Split vote** | An election where no candidate gets a majority |
| **Linearizability** | Operations appear atomic, in an order consistent with real time |
| **Liveness vs. safety** | "Something good eventually happens" vs. "nothing bad ever happens". Raft is always safe and live when the network is reasonable. |
| **State machine replication** | Same deterministic commands in the same order → same state |
