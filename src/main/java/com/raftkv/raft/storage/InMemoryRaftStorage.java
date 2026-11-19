package com.raftkv.raft.storage;

import com.raftkv.raft.LogEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Non-durable storage for local experiments and tests.
 *
 * <p>A process restart loses everything, so this is NOT crash-safe. In tests, a node
 * "restart" reuses the same instance to simulate a disk that survived the crash.
 */
public class InMemoryRaftStorage implements RaftStorage {

    private HardState hardState = HardState.INITIAL;
    private final List<LogEntry> log = new ArrayList<>();

    @Override
    public synchronized HardState loadHardState() {
        return hardState;
    }

    @Override
    public synchronized void saveHardState(HardState state) {
        this.hardState = state;
    }

    @Override
    public synchronized List<LogEntry> loadLog() {
        return new ArrayList<>(log);
    }

    @Override
    public synchronized void appendEntries(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public synchronized void deleteEntriesFrom(long fromIndex) {
        int from = (int) (fromIndex - 1);
        if (from < log.size()) {
            log.subList(from, log.size()).clear();
        }
    }
}
