package com.raftkv.raft;

/**
 * At any moment every Raft node is in exactly one of these three states.
 *
 * <pre>
 *                 times out,                       receives votes from
 *   starts up     starts election                  a majority of nodes
 *  ----------> FOLLOWER ----------> CANDIDATE ------------------------> LEADER
 *                 ^  ^                 |  |                               |
 *                 |  |  discovers      |  | times out,                    |
 *                 |  |  current leader |  | new election                  |
 *                 |  +-----------------+  +---> (stays CANDIDATE)         |
 *                 |       or a higher term                                |
 *                 +--------------- discovers a node with a higher term ---+
 * </pre>
 */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
