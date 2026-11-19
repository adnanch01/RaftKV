package com.raftkv.raft;

/** Thrown when a client sends a write to a node that is not the leader. */
public class NotLeaderException extends RuntimeException {

    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super(leaderId == null ? "No leader is currently known" : "Not the leader; current leader is " + leaderId);
        this.leaderId = leaderId;
    }

    /** The leader this node believes in, or null during an election. */
    public String getLeaderId() {
        return leaderId;
    }
}
