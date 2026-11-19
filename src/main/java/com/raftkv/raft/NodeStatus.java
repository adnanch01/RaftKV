package com.raftkv.raft;

import java.util.Map;

/** A read-only snapshot of a node's Raft state, for the status endpoint and tests. */
public record NodeStatus(
        String nodeId,
        Role role,
        long currentTerm,
        String votedFor,
        String leaderId,
        long commitIndex,
        long lastApplied,
        long lastLogIndex,
        long lastLogTerm,
        Map<String, Long> nextIndex,
        Map<String, Long> matchIndex) {
}
