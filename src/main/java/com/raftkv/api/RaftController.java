package com.raftkv.api;

import com.raftkv.raft.LogEntry;
import com.raftkv.raft.NodeStatus;
import com.raftkv.raft.RaftNode;
import com.raftkv.raft.transport.NetworkSimulator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Endpoints for inspecting Raft internals and simulating failures. */
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode node;
    private final NetworkSimulator network;

    public RaftController(RaftNode node, NetworkSimulator network) {
        this.node = node;
        this.network = network;
    }

    /** Role, term, vote, commit index, and (on the leader) per-follower replication progress. */
    @GetMapping("/status")
    public Map<String, Object> status() {
        NodeStatus s = node.status();
        return Map.of("raft", s, "isolated", network.isIsolated());
    }

    /** Raw log entries, with commands decoded as UTF-8 text. */
    @GetMapping("/log")
    public List<Map<String, Object>> log(@RequestParam(defaultValue = "1") long from,
                                         @RequestParam(defaultValue = "50") int limit) {
        List<LogEntry> entries = node.logEntries(from, Math.min(limit, 500));
        long commitIndex = node.status().commitIndex();
        return entries.stream().map(e -> Map.<String, Object>of(
                "index", e.index(),
                "term", e.term(),
                "committed", e.index() <= commitIndex,
                "command", e.isNoOp() ? "<no-op>" : new String(e.command(), StandardCharsets.UTF_8)
        )).toList();
    }

    /** Simulates unplugging this node's network cable: all Raft RPCs in and out are dropped. */
    @PostMapping("/admin/isolate")
    public Map<String, Object> isolate() {
        network.setIsolated(true);
        return Map.of("node", node.getNodeId(), "isolated", true);
    }

    /** Reconnects the node to the cluster. */
    @PostMapping("/admin/heal")
    public Map<String, Object> heal() {
        network.setIsolated(false);
        return Map.of("node", node.getNodeId(), "isolated", false);
    }
}
