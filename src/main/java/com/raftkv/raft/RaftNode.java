package com.raftkv.raft;

import com.raftkv.raft.rpc.AppendEntriesRequest;
import com.raftkv.raft.rpc.AppendEntriesResponse;
import com.raftkv.raft.rpc.LogEntryProto;
import com.raftkv.raft.rpc.RequestVoteRequest;
import com.raftkv.raft.rpc.RequestVoteResponse;
import com.raftkv.raft.storage.RaftStorage;
import com.raftkv.raft.storage.RaftStorage.HardState;
import com.raftkv.raft.transport.RaftTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single Raft node: leader election, log replication, and commitment.
 *
 * <h2>Concurrency model</h2>
 * One {@link ReentrantLock} guards all Raft state. Every event (a timer tick, an incoming
 * RPC, an RPC response, a client proposal) takes the lock, updates state, and releases
 * it. Outgoing RPCs are always sent <em>after</em> the lock is released, so a slow or
 * dead peer can never stall this node. Responses arrive on other threads and re-take the
 * lock, then check that the world has not moved on in the meantime (same term? still
 * leader?). That re-check is what keeps concurrent state transitions safe.
 *
 * <h2>Timers</h2>
 * A single "ticker" thread wakes every few milliseconds. A leader uses it to send
 * heartbeats. A follower or candidate uses it to start an election once its randomized
 * election deadline passes.
 *
 * <p>Comments in the form "(Fig. 2)" or "(§5.4)" point at the matching part of the
 * Raft paper, "In Search of an Understandable Consensus Algorithm" (Ongaro &amp;
 * Ousterhout, 2014).
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private static final int MAX_ENTRIES_PER_APPEND = 100;
    private static final long TICK_MILLIS = 10;

    /** Static configuration of one node. {@code peerIds} excludes the node itself. */
    public record Config(String nodeId,
                         List<String> peerIds,
                         Duration electionTimeoutMin,
                         Duration electionTimeoutMax,
                         Duration heartbeatInterval) {
    }

    private record PendingProposal(long term, CompletableFuture<Object> future) {
    }

    private final Config config;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final ReentrantLock lock = new ReentrantLock();
    private ScheduledExecutorService ticker;

    // ---- Persistent state on all servers (Fig. 2): written to storage before answering RPCs
    private long currentTerm;
    private String votedFor;
    // (the log itself lives in raftLog)

    // ---- Volatile state on all servers (Fig. 2)
    private long commitIndex;  // highest index known to be committed
    private long lastApplied;  // highest index applied to the state machine
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long electionDeadlineNanos;

    // ---- Volatile state on candidates
    private final Set<String> votesGranted = new HashSet<>();

    // ---- Volatile state on leaders (Fig. 2), reinitialized after each election
    private final Map<String, Long> nextIndex = new HashMap<>();  // next entry to send to each peer
    private final Map<String, Long> matchIndex = new HashMap<>(); // highest entry known replicated on each peer
    private long nextHeartbeatNanos;

    // Client proposals waiting for their entry to be committed and applied, keyed by log index
    private final Map<Long, PendingProposal> pending = new HashMap<>();
    private boolean running;

    public RaftNode(Config config, RaftTransport transport, RaftStorage storage, StateMachine stateMachine) {
        this.config = config;
        this.transport = transport;
        this.storage = storage;
        this.stateMachine = stateMachine;

        // Recover persistent state after a restart.
        HardState hs = storage.loadHardState();
        this.currentTerm = hs.currentTerm();
        this.votedFor = hs.votedFor();
        this.raftLog = new RaftLog(storage);
        // commitIndex and lastApplied start at 0 on restart. The state machine is rebuilt by
        // re-applying the log once the leader tells us what is committed. (Snapshots would let
        // us skip replaying the whole log; see docs/LEARNING_GUIDE.md.)
    }

    // =====================================================================================
    // Lifecycle
    // =====================================================================================

    public void start() {
        lock.lock();
        try {
            running = true;
            resetElectionDeadline();
            log.info("[{}] starting as FOLLOWER in term {} with {} log entries",
                    config.nodeId(), currentTerm, raftLog.lastIndex());
        } finally {
            lock.unlock();
        }
        ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-ticker-" + config.nodeId());
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(this::tick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        lock.lock();
        try {
            running = false;
            failAllPending(new ProposalDroppedException("Node " + config.nodeId() + " is shutting down"));
        } finally {
            lock.unlock();
        }
        if (ticker != null) {
            ticker.shutdownNow();
        }
    }

    private void tick() {
        try {
            boolean election = false;
            boolean heartbeat = false;
            lock.lock();
            try {
                if (!running) {
                    return;
                }
                long now = System.nanoTime();
                if (role == Role.LEADER) {
                    if (now >= nextHeartbeatNanos) {
                        heartbeat = true;
                        nextHeartbeatNanos = now + config.heartbeatInterval().toNanos();
                    }
                } else if (now >= electionDeadlineNanos) {
                    election = true;
                }
            } finally {
                lock.unlock();
            }
            if (election) {
                startElection();
            }
            if (heartbeat) {
                broadcastAppendEntries();
            }
        } catch (Throwable t) {
            // A scheduled task that throws is silently cancelled, which would freeze the node.
            log.error("[{}] tick failed", config.nodeId(), t);
        }
    }

    /**
     * Picks a new random deadline in [min, max]. Randomization is the key to liveness:
     * if every node used the same timeout they would all become candidates at once,
     * split the vote, and could repeat that forever. With random timeouts, one node
     * usually times out first and wins before anyone else wakes up (§5.2).
     */
    private void resetElectionDeadline() {
        long min = config.electionTimeoutMin().toNanos();
        long max = config.electionTimeoutMax().toNanos();
        electionDeadlineNanos = System.nanoTime() + ThreadLocalRandom.current().nextLong(min, max + 1);
    }

    // =====================================================================================
    // Leader election (§5.2)
    // =====================================================================================

    private void startElection() {
        RequestVoteRequest request;
        long electionTerm;
        lock.lock();
        try {
            if (!running || role == Role.LEADER || System.nanoTime() < electionDeadlineNanos) {
                return; // state changed between tick() and here
            }
            // Become a candidate: bump the term, vote for ourselves, persist both.
            role = Role.CANDIDATE;
            currentTerm++;
            votedFor = config.nodeId();
            leaderId = null;
            persistHardState();
            votesGranted.clear();
            votesGranted.add(config.nodeId());
            resetElectionDeadline(); // if this election splits, we retry after a new random timeout
            electionTerm = currentTerm;

            log.info("[{}] election timeout, becoming CANDIDATE for term {}", config.nodeId(), currentTerm);

            if (hasMajority(votesGranted.size())) { // single-node cluster
                becomeLeader();
                return;
            }
            request = RequestVoteRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setCandidateId(config.nodeId())
                    .setLastLogIndex(raftLog.lastIndex())
                    .setLastLogTerm(raftLog.lastTerm())
                    .build();
        } finally {
            lock.unlock();
        }

        for (String peer : config.peerIds()) {
            transport.requestVote(peer, request).whenComplete((response, error) -> {
                if (error == null) {
                    handleVoteResponse(electionTerm, response);
                } else {
                    log.debug("[{}] RequestVote to {} failed: {}", config.nodeId(), peer, error.toString());
                }
            });
        }
    }

    /** RequestVote RPC receiver (Fig. 2). */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        lock.lock();
        try {
            ensureRunning();
            // Rule for all servers: a higher term means we are out of date. Step down.
            if (req.getTerm() > currentTerm) {
                becomeFollower(req.getTerm());
            }

            boolean granted = false;
            if (req.getTerm() == currentTerm
                    && (votedFor == null || votedFor.equals(req.getCandidateId()))
                    && candidateLogIsUpToDate(req.getLastLogTerm(), req.getLastLogIndex())) {
                votedFor = req.getCandidateId();
                persistHardState(); // must reach disk before we reply, or we could vote twice after a crash
                granted = true;
                // Granting a vote counts as hearing from a viable leader-to-be, so stay a follower.
                resetElectionDeadline();
            }

            log.debug("[{}] vote for {} in term {}: {}", config.nodeId(), req.getCandidateId(), req.getTerm(),
                    granted ? "GRANTED" : "DENIED");
            return RequestVoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(granted)
                    .setVoterId(config.nodeId())
                    .build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The election restriction (§5.4.1). A voter only accepts a candidate whose log is at
     * least as up to date as its own: a higher last term, or the same last term and an
     * equal or longer log. Every committed entry exists on a majority, and any two
     * majorities overlap, so a candidate missing a committed entry can never collect
     * enough votes. This guarantees a new leader already holds every committed entry
     * (Leader Completeness).
     */
    private boolean candidateLogIsUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = raftLog.lastTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= raftLog.lastIndex();
    }

    private void handleVoteResponse(long electionTerm, RequestVoteResponse response) {
        boolean wonElection = false;
        lock.lock();
        try {
            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm());
                return;
            }
            // Ignore stale responses from an earlier election or after we already changed role.
            if (role != Role.CANDIDATE || currentTerm != electionTerm) {
                return;
            }
            if (response.getVoteGranted()) {
                votesGranted.add(response.getVoterId()); // a Set, so duplicate responses cannot double count
                if (hasMajority(votesGranted.size())) {
                    becomeLeader();
                    wonElection = true;
                }
            }
        } finally {
            lock.unlock();
        }
        if (wonElection) {
            broadcastAppendEntries(); // announce leadership right away
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = config.nodeId();
        votesGranted.clear();
        for (String peer : config.peerIds()) {
            nextIndex.put(peer, raftLog.lastIndex() + 1); // optimistic: assume followers match us
            matchIndex.put(peer, 0L);                     // pessimistic: nothing confirmed yet
        }
        nextHeartbeatNanos = System.nanoTime() + config.heartbeatInterval().toNanos();
        log.info("[{}] *** became LEADER for term {} with votes from a majority ***", config.nodeId(), currentTerm);

        // Append a no-op entry from the new term. A leader may only count replicas to commit
        // entries from its OWN term (§5.4.2, Figure 8). Committing this no-op indirectly
        // commits every earlier entry, so leftovers from previous terms do not hang forever
        // waiting for a client write.
        appendToOwnLog(new byte[0]);
        maybeAdvanceCommitIndex(); // a single-node cluster commits immediately
    }

    /** Must be called with the lock held. */
    private void becomeFollower(long term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;   // a new term means a fresh vote
            leaderId = null;
            persistHardState();
        }
        if (role != Role.FOLLOWER) {
            log.info("[{}] stepping down from {} to FOLLOWER in term {}", config.nodeId(), role, currentTerm);
        }
        role = Role.FOLLOWER;
        votesGranted.clear();
    }

    // =====================================================================================
    // Log replication (§5.3)
    // =====================================================================================

    /**
     * Entry point for client commands. Appends the command to the leader's log and
     * returns a future. The future completes once the entry has been committed (stored
     * on a majority) and applied to the state machine.
     *
     * @throws NotLeaderException if this node is not the leader
     */
    public CompletableFuture<Object> submit(byte[] command) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        lock.lock();
        try {
            if (!running || role != Role.LEADER) {
                throw new NotLeaderException(leaderId);
            }
            long index = appendToOwnLog(command);
            pending.put(index, new PendingProposal(currentTerm, future));
            maybeAdvanceCommitIndex(); // single-node cluster
        } finally {
            lock.unlock();
        }
        broadcastAppendEntries(); // replicate now rather than waiting for the next heartbeat
        return future;
    }

    private long appendToOwnLog(byte[] command) {
        long index = raftLog.lastIndex() + 1;
        raftLog.append(List.of(new LogEntry(index, currentTerm, command)));
        return index;
    }

    private void broadcastAppendEntries() {
        for (String peer : config.peerIds()) {
            sendAppendEntries(peer);
        }
    }

    /**
     * Sends a peer everything from nextIndex[peer] onward (up to a batch limit), plus the
     * (prevLogIndex, prevLogTerm) of the entry just before them. With no new entries this
     * is a heartbeat.
     */
    private void sendAppendEntries(String peer) {
        AppendEntriesRequest request;
        long term;
        lock.lock();
        try {
            if (!running || role != Role.LEADER) {
                return;
            }
            long next = nextIndex.get(peer);
            long prevIndex = next - 1;
            List<LogEntryProto> entries = new ArrayList<>();
            for (LogEntry e : raftLog.slice(next, MAX_ENTRIES_PER_APPEND)) {
                entries.add(e.toProto());
            }
            request = AppendEntriesRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setLeaderId(config.nodeId())
                    .setPrevLogIndex(prevIndex)
                    .setPrevLogTerm(raftLog.termAt(prevIndex))
                    .addAllEntries(entries)
                    .setLeaderCommit(commitIndex)
                    .build();
            term = currentTerm;
        } finally {
            lock.unlock();
        }

        transport.appendEntries(peer, request).whenComplete((response, error) -> {
            if (error == null) {
                handleAppendEntriesResponse(peer, term, request, response);
            } else {
                // Peer unreachable. Nothing to do: the next heartbeat retries automatically.
                log.debug("[{}] AppendEntries to {} failed: {}", config.nodeId(), peer, error.toString());
            }
        });
    }

    /** AppendEntries RPC receiver (Fig. 2). */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        lock.lock();
        try {
            ensureRunning();
            AppendEntriesResponse.Builder response = AppendEntriesResponse.newBuilder()
                    .setFollowerId(config.nodeId());

            // 1. Reply false if term < currentTerm. This is a deposed leader that has not noticed yet.
            if (req.getTerm() < currentTerm) {
                return response.setTerm(currentTerm).setSuccess(false).build();
            }

            // A valid leader exists for this term. Candidates in the same term give up.
            if (req.getTerm() > currentTerm || role != Role.FOLLOWER) {
                becomeFollower(req.getTerm());
            }
            leaderId = req.getLeaderId();
            resetElectionDeadline(); // the heartbeat keeps us from starting an election
            response.setTerm(currentTerm);

            // 2. Consistency check: we must already hold the entry right before the new ones.
            long prevIndex = req.getPrevLogIndex();
            if (prevIndex > raftLog.lastIndex()) {
                // Our log is too short. Ask the leader to back up to our end.
                return response.setSuccess(false)
                        .setConflictIndex(raftLog.lastIndex() + 1)
                        .setConflictTerm(0)
                        .build();
            }
            if (raftLog.termAt(prevIndex) != req.getPrevLogTerm()) {
                // We hold a different entry there. Report its term and where that term starts in
                // our log, so the leader can skip the whole conflicting term in one round trip.
                long conflictTerm = raftLog.termAt(prevIndex);
                return response.setSuccess(false)
                        .setConflictTerm(conflictTerm)
                        .setConflictIndex(raftLog.firstIndexOfTerm(conflictTerm, prevIndex))
                        .build();
            }

            // 3 & 4. Append new entries, deleting our entries only where they actually conflict.
            // Do NOT truncate blindly: a delayed, out-of-order RPC must never erase entries that
            // a newer RPC already appended (and that might already be committed).
            List<LogEntry> toAppend = new ArrayList<>();
            long index = prevIndex;
            for (LogEntryProto proto : req.getEntriesList()) {
                index++;
                if (toAppend.isEmpty() && index <= raftLog.lastIndex()) {
                    if (raftLog.termAt(index) == proto.getTerm()) {
                        continue; // we already have this exact entry (Log Matching Property)
                    }
                    log.info("[{}] log conflict at index {}: truncating our suffix", config.nodeId(), index);
                    truncateLogFrom(index);
                }
                toAppend.add(LogEntry.fromProto(proto));
            }
            raftLog.append(toAppend);

            // 5. Advance commitIndex. Only up to the last entry this RPC vouched for, because
            // anything beyond it in our log has not been verified against the leader's.
            long lastNewIndex = prevIndex + req.getEntriesCount();
            if (req.getLeaderCommit() > commitIndex) {
                commitIndex = Math.max(commitIndex, Math.min(req.getLeaderCommit(), lastNewIndex));
                applyCommittedEntries();
            }

            return response.setSuccess(true).setMatchIndex(lastNewIndex).build();
        } finally {
            lock.unlock();
        }
    }

    private void handleAppendEntriesResponse(String peer, long requestTerm,
                                             AppendEntriesRequest request, AppendEntriesResponse response) {
        boolean sendMore = false;
        lock.lock();
        try {
            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm()); // a newer leader or candidate exists
                return;
            }
            if (role != Role.LEADER || currentTerm != requestTerm) {
                return; // stale response from an older term
            }

            if (response.getSuccess()) {
                long match = response.getMatchIndex();
                if (match > matchIndex.get(peer)) {
                    matchIndex.put(peer, match);
                }
                nextIndex.put(peer, matchIndex.get(peer) + 1);
                maybeAdvanceCommitIndex();
            } else {
                // The follower's log diverges. Back nextIndex up using its hint and retry.
                long newNext;
                if (response.getConflictTerm() > 0) {
                    long lastOfTerm = raftLog.lastIndexOfTerm(response.getConflictTerm());
                    newNext = lastOfTerm > 0 ? lastOfTerm + 1 : response.getConflictIndex();
                } else {
                    newNext = response.getConflictIndex();
                }
                // Never move below what we know the follower matches, and never below 1.
                newNext = Math.max(Math.max(1, matchIndex.get(peer) + 1), Math.min(newNext, request.getPrevLogIndex()));
                nextIndex.put(peer, newNext);
                log.debug("[{}] {} rejected AppendEntries at prevIndex {}, retrying from {}",
                        config.nodeId(), peer, request.getPrevLogIndex(), newNext);
            }
            // Keep going while the follower is behind (or after a rejection) instead of
            // waiting a full heartbeat interval for each batch.
            sendMore = nextIndex.get(peer) <= raftLog.lastIndex() || !response.getSuccess();
        } finally {
            lock.unlock();
        }
        if (sendMore) {
            sendAppendEntries(peer);
        }
    }

    /**
     * Leader commit rule (Fig. 2). If some N > commitIndex is stored on a majority
     * (matchIndex[i] >= N for a majority) and log[N].term == currentTerm, then
     * commitIndex = N.
     *
     * <p>The {@code term == currentTerm} condition matters. Without it, an entry from an
     * older term that sits on a majority could still be overwritten by a future leader
     * (Figure 8 in the paper).
     */
    private void maybeAdvanceCommitIndex() {
        for (long n = raftLog.lastIndex(); n > commitIndex; n--) {
            if (raftLog.termAt(n) != currentTerm) {
                break; // earlier entries have even older terms
            }
            int replicas = 1; // the leader itself
            for (long m : matchIndex.values()) {
                if (m >= n) {
                    replicas++;
                }
            }
            if (hasMajority(replicas)) {
                commitIndex = n;
                log.debug("[{}] commitIndex advanced to {}", config.nodeId(), n);
                applyCommittedEntries();
                return;
            }
        }
    }

    /** Applies entries (lastApplied, commitIndex] to the state machine, in order. */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = raftLog.get(lastApplied);
            Object result = null;
            Throwable failure = null;
            if (!entry.isNoOp()) {
                try {
                    result = stateMachine.apply(entry.index(), entry.command());
                } catch (RuntimeException e) {
                    failure = e;
                }
            }
            PendingProposal proposal = pending.remove(lastApplied);
            if (proposal != null) {
                if (proposal.term() != entry.term()) {
                    // A different leader's entry was committed at this index. Ours was lost.
                    proposal.future().completeExceptionally(
                            new ProposalDroppedException("Entry " + lastApplied + " was replaced by a new leader"));
                } else if (failure != null) {
                    proposal.future().completeExceptionally(failure);
                } else {
                    proposal.future().complete(result);
                }
            }
        }
    }

    private void truncateLogFrom(long index) {
        raftLog.truncateFrom(index);
        // Any client waiting on an entry at or after this index lost it. Tell them now.
        Iterator<Map.Entry<Long, PendingProposal>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, PendingProposal> e = it.next();
            if (e.getKey() >= index) {
                e.getValue().future().completeExceptionally(
                        new ProposalDroppedException("Entry " + e.getKey() + " was overwritten by a new leader"));
                it.remove();
            }
        }
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    private boolean hasMajority(int count) {
        int clusterSize = config.peerIds().size() + 1;
        return count > clusterSize / 2; // 5 nodes -> 3 votes; tolerates 2 failures
    }

    /** A stopped (crashed) node must not answer RPCs or touch its storage. */
    private void ensureRunning() {
        if (!running) {
            throw new IllegalStateException("Node " + config.nodeId() + " is not running");
        }
    }

    private void persistHardState() {
        storage.saveHardState(new HardState(currentTerm, votedFor));
    }

    private void failAllPending(RuntimeException cause) {
        pending.values().forEach(p -> p.future().completeExceptionally(cause));
        pending.clear();
    }

    public String getNodeId() {
        return config.nodeId();
    }

    public NodeStatus status() {
        lock.lock();
        try {
            boolean leader = role == Role.LEADER;
            return new NodeStatus(config.nodeId(), role, currentTerm, votedFor, leaderId, commitIndex, lastApplied,
                    raftLog.lastIndex(), raftLog.lastTerm(),
                    leader ? Map.copyOf(nextIndex) : Map.of(),
                    leader ? Map.copyOf(matchIndex) : Map.of());
        } finally {
            lock.unlock();
        }
    }

    /** A copy of log entries in [fromIndex, fromIndex + limit), for inspection. */
    public List<LogEntry> logEntries(long fromIndex, int limit) {
        lock.lock();
        try {
            return raftLog.slice(Math.max(1, fromIndex), limit);
        } finally {
            lock.unlock();
        }
    }
}
