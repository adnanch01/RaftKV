package com.raftkv.raft.storage;

import com.raftkv.raft.LogEntry;

import java.util.List;

/**
 * Durable storage for the state Raft requires to survive a crash.
 *
 * <p>Figure 2 of the Raft paper lists exactly three pieces of persistent state:
 * {@code currentTerm}, {@code votedFor} and the {@code log[]}. They must be written to
 * stable storage <em>before</em> the node responds to an RPC. Otherwise a node could
 * crash, restart, and vote twice in the same term, or "forget" an entry it already told
 * the leader it had stored. Either would break safety.
 *
 * <p>Everything else (commitIndex, lastApplied, nextIndex, matchIndex) is volatile and
 * gets rebuilt after a restart.
 */
public interface RaftStorage {

    /** The term and vote of this node. Called "hard state" in etcd's Raft library. */
    record HardState(long currentTerm, String votedFor) {
        public static final HardState INITIAL = new HardState(0, null);
    }

    HardState loadHardState();

    void saveHardState(HardState state);

    /** Returns the whole log in index order (1, 2, 3, ...). */
    List<LogEntry> loadLog();

    /** Appends entries; their indexes always directly follow the current last index. */
    void appendEntries(List<LogEntry> entries);

    /** Deletes the entry at {@code fromIndex} and everything after it (resolves log conflicts). */
    void deleteEntriesFrom(long fromIndex);
}
