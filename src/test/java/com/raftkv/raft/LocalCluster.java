package com.raftkv.raft;

import com.raftkv.kv.KeyValueStateMachine;
import com.raftkv.raft.rpc.AppendEntriesRequest;
import com.raftkv.raft.rpc.AppendEntriesResponse;
import com.raftkv.raft.rpc.RequestVoteRequest;
import com.raftkv.raft.rpc.RequestVoteResponse;
import com.raftkv.raft.storage.InMemoryRaftStorage;
import com.raftkv.raft.transport.RaftTransport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * An in-process cluster for tests. All nodes talk over a simulated network that can:
 * <ul>
 *   <li>crash a node ({@link #crash}) and restart it from its "disk" ({@link #restart})</li>
 *   <li>partition the cluster into groups that cannot reach each other ({@link #partition})</li>
 * </ul>
 */
class LocalCluster implements AutoCloseable {

    final List<String> ids = new ArrayList<>();
    final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    final Map<String, KeyValueStateMachine> stateMachines = new ConcurrentHashMap<>();
    private final Map<String, InMemoryRaftStorage> disks = new HashMap<>();
    private final Set<String> crashed = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> partitionGroup = new ConcurrentHashMap<>();
    private final ExecutorService network = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "sim-network");
        t.setDaemon(true);
        return t;
    });

    LocalCluster(int size) {
        for (int i = 1; i <= size; i++) {
            ids.add("n" + i);
        }
        for (String id : ids) {
            disks.put(id, new InMemoryRaftStorage());
            startNode(id);
        }
    }

    private void startNode(String id) {
        List<String> peers = ids.stream().filter(p -> !p.equals(id)).toList();
        RaftNode.Config config = new RaftNode.Config(id, peers,
                Duration.ofMillis(150), Duration.ofMillis(300), Duration.ofMillis(40));
        KeyValueStateMachine sm = new KeyValueStateMachine();
        RaftNode node = new RaftNode(config, new SimTransport(id), disks.get(id), sm);
        nodes.put(id, node);
        stateMachines.put(id, sm);
        node.start();
    }

    /** Kills a node: it stops and all messages to and from it are dropped. Its disk survives. */
    void crash(String id) {
        crashed.add(id);
        nodes.get(id).stop();
    }

    /** Restarts a crashed node from its persisted term, vote, and log. Memory state is rebuilt. */
    void restart(String id) {
        crashed.remove(id);
        startNode(id);
    }

    /** Splits the cluster: nodes can only talk to nodes in the same group. */
    @SafeVarargs
    final void partition(List<String>... groups) {
        for (int g = 0; g < groups.length; g++) {
            for (String id : groups[g]) {
                partitionGroup.put(id, g);
            }
        }
    }

    void heal() {
        partitionGroup.clear();
    }

    private boolean canTalk(String from, String to) {
        if (crashed.contains(from) || crashed.contains(to)) {
            return false;
        }
        return partitionGroup.getOrDefault(from, 0).equals(partitionGroup.getOrDefault(to, 0));
    }

    private class SimTransport implements RaftTransport {
        private final String self;

        SimTransport(String self) {
            this.self = self;
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String peer, RequestVoteRequest req) {
            return deliver(peer, n -> n.handleRequestVote(req));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String peer, AppendEntriesRequest req) {
            return deliver(peer, n -> n.handleAppendEntries(req));
        }

        private <T> CompletableFuture<T> deliver(String peer, Function<RaftNode, T> handler) {
            return CompletableFuture.supplyAsync(() -> {
                // Check reachability on both the request and the response leg.
                if (!canTalk(self, peer)) {
                    throw new RuntimeException("unreachable: " + self + " -> " + peer);
                }
                T response = handler.apply(nodes.get(peer));
                if (!canTalk(peer, self)) {
                    throw new RuntimeException("response lost: " + peer + " -> " + self);
                }
                return response;
            }, network);
        }
    }

    // ---------------------------------------------------------------- query helpers

    List<String> liveIds() {
        return ids.stream().filter(id -> !crashed.contains(id)).toList();
    }

    /** Leaders among the given nodes (there should be at most one per term). */
    List<String> leadersAmong(List<String> candidates) {
        return candidates.stream()
                .filter(id -> nodes.get(id).status().role() == Role.LEADER)
                .toList();
    }

    /** Waits for exactly one leader among {@code group} and returns it. */
    String awaitLeader(List<String> group) {
        await(() -> {
            List<String> leaders = leadersAmong(group);
            if (leaders.size() != 1) {
                return false;
            }
            // Every live member must also agree on who leads, after the first heartbeat.
            long term = nodes.get(leaders.get(0)).status().currentTerm();
            return group.stream().allMatch(id -> {
                NodeStatus s = nodes.get(id).status();
                return s.currentTerm() == term && leaders.get(0).equals(s.leaderId());
            });
        }, "a single agreed-upon leader among " + group);
        return leadersAmong(group).get(0);
    }

    String awaitLeader() {
        return awaitLeader(liveIds());
    }

    Optional<String> valueOn(String id, String key) {
        return Optional.ofNullable(stateMachines.get(id).getLocal(key));
    }

    static void await(BooleanSupplier condition, String description) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted", e);
            }
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    @Override
    public void close() {
        nodes.values().forEach(RaftNode::stop);
        network.shutdownNow();
    }
}
