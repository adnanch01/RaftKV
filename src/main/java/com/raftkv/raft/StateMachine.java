package com.raftkv.raft;

/**
 * The application that Raft replicates.
 *
 * <p>Raft guarantees every node applies the same commands in the same order. If
 * {@link #apply} is deterministic (same input, same output, no clocks or randomness),
 * every node ends up in the same state. This idea is called State Machine Replication.
 */
public interface StateMachine {

    /**
     * Applies a committed command. Called exactly once per log index, in index order.
     *
     * @return a result handed back to the client that submitted the command
     */
    Object apply(long index, byte[] command);
}
