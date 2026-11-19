package com.raftkv.raft;

/**
 * The entry a client proposed was overwritten before it could commit.
 *
 * <p>This happens when the leader that accepted it loses leadership, and a new leader
 * replaces the entry with a different one. The command was NOT applied, so the client
 * may safely retry.
 */
public class ProposalDroppedException extends RuntimeException {
    public ProposalDroppedException(String message) {
        super(message);
    }
}
