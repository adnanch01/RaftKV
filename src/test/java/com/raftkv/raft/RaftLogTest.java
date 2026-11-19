package com.raftkv.raft;

import com.raftkv.raft.storage.InMemoryRaftStorage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RaftLogTest {

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, ("cmd" + index).getBytes());
    }

    @Test
    void sentinelAtIndexZero() {
        RaftLog log = new RaftLog(new InMemoryRaftStorage());
        assertThat(log.lastIndex()).isZero();
        assertThat(log.termAt(0)).isZero();
        assertThat(log.lastTerm()).isZero();
    }

    @Test
    void truncateRemovesSuffixFromMemoryAndStorage() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        RaftLog log = new RaftLog(storage);
        log.append(List.of(entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2)));

        log.truncateFrom(3);

        assertThat(log.lastIndex()).isEqualTo(2);
        assertThat(new RaftLog(storage).lastIndex()).isEqualTo(2); // a reload sees the same thing
    }

    @Test
    void termSearchesForFastBackup() {
        RaftLog log = new RaftLog(new InMemoryRaftStorage());
        log.append(List.of(entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2), entry(5, 4)));

        assertThat(log.firstIndexOfTerm(2, 4)).isEqualTo(2);
        assertThat(log.lastIndexOfTerm(2)).isEqualTo(4);
        assertThat(log.lastIndexOfTerm(3)).isZero();
    }

    @Test
    void sliceIsBounded() {
        RaftLog log = new RaftLog(new InMemoryRaftStorage());
        log.append(List.of(entry(1, 1), entry(2, 1), entry(3, 1)));

        assertThat(log.slice(2, 10)).extracting(LogEntry::index).containsExactly(2L, 3L);
        assertThat(log.slice(1, 2)).extracting(LogEntry::index).containsExactly(1L, 2L);
        assertThat(log.slice(4, 10)).isEmpty();
    }
}
