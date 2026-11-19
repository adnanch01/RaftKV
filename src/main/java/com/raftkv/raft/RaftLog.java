package com.raftkv.raft;

import com.raftkv.raft.storage.RaftStorage;

import java.util.ArrayList;
import java.util.List;

/**
 * The replicated log: an in-memory list mirrored to durable {@link RaftStorage}.
 *
 * <p>Indexes are 1-based. Index 0 is a virtual sentinel with term 0, so
 * {@code termAt(0) == 0}. That makes the AppendEntries consistency check work for the
 * very first entry without special cases.
 *
 * <p>Not thread-safe. {@link RaftNode} guards every access with its lock.
 */
public final class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private final RaftStorage storage;

    public RaftLog(RaftStorage storage) {
        this.storage = storage;
        List<LogEntry> persisted = storage.loadLog();
        for (int i = 0; i < persisted.size(); i++) {
            if (persisted.get(i).index() != i + 1) {
                throw new IllegalStateException("Persisted log is not contiguous at position " + (i + 1));
            }
        }
        entries.addAll(persisted);
    }

    public long lastIndex() {
        return entries.size();
    }

    public long lastTerm() {
        return termAt(lastIndex());
    }

    /** Term of the entry at {@code index}; 0 for the sentinel at index 0. */
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        return get(index).term();
    }

    public LogEntry get(long index) {
        if (index < 1 || index > lastIndex()) {
            throw new IndexOutOfBoundsException("No log entry at index " + index + " (lastIndex=" + lastIndex() + ")");
        }
        return entries.get((int) (index - 1));
    }

    /** Up to {@code max} entries starting at {@code fromIndex} (inclusive). */
    public List<LogEntry> slice(long fromIndex, int max) {
        if (fromIndex > lastIndex()) {
            return List.of();
        }
        int from = (int) (fromIndex - 1);
        int to = (int) Math.min(entries.size(), (long) from + max);
        return List.copyOf(entries.subList(from, to));
    }

    /** Persists first, then updates memory, so memory never holds entries the disk lacks. */
    public void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return;
        }
        if (newEntries.get(0).index() != lastIndex() + 1) {
            throw new IllegalArgumentException("Append must start at " + (lastIndex() + 1)
                    + " but started at " + newEntries.get(0).index());
        }
        storage.appendEntries(newEntries);
        entries.addAll(newEntries);
    }

    /** Removes the entry at {@code fromIndex} and all following entries. */
    public void truncateFrom(long fromIndex) {
        if (fromIndex > lastIndex()) {
            return;
        }
        storage.deleteEntriesFrom(fromIndex);
        entries.subList((int) (fromIndex - 1), entries.size()).clear();
    }

    /** First index holding {@code term}, scanning backwards from {@code startIndex}. */
    public long firstIndexOfTerm(long term, long startIndex) {
        long i = startIndex;
        while (i > 1 && termAt(i - 1) == term) {
            i--;
        }
        return i;
    }

    /** Last index holding {@code term}, or 0 if the log has no entry from that term. */
    public long lastIndexOfTerm(long term) {
        for (long i = lastIndex(); i >= 1; i--) {
            long t = termAt(i);
            if (t == term) {
                return i;
            }
            if (t < term) {
                return 0; // terms only increase along the log
            }
        }
        return 0;
    }
}
