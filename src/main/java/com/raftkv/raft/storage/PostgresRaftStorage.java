package com.raftkv.raft.storage;

import com.raftkv.raft.LogEntry;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * Stores the Raft hard state and log in PostgreSQL.
 *
 * <p>Each row has a {@code node_id} column, so several nodes can share one database in
 * development. In docker-compose, each node gets its own database to mimic a real
 * deployment, where every node owns its own disk.
 *
 * <p>Every write runs synchronously before the node answers the RPC that caused it.
 * That ordering is the durability guarantee Raft depends on.
 */
public class PostgresRaftStorage implements RaftStorage {

    private final JdbcTemplate jdbc;
    private final String nodeId;

    public PostgresRaftStorage(JdbcTemplate jdbc, String nodeId) {
        this.jdbc = jdbc;
        this.nodeId = nodeId;
        createSchema();
    }

    private void createSchema() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS raft_hard_state (
                    node_id      VARCHAR(64) PRIMARY KEY,
                    current_term BIGINT      NOT NULL,
                    voted_for    VARCHAR(64)
                )""");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS raft_log (
                    node_id   VARCHAR(64) NOT NULL,
                    log_index BIGINT      NOT NULL,
                    term      BIGINT      NOT NULL,
                    command   BYTEA       NOT NULL,
                    PRIMARY KEY (node_id, log_index)
                )""");
    }

    @Override
    public HardState loadHardState() {
        List<HardState> rows = jdbc.query(
                "SELECT current_term, voted_for FROM raft_hard_state WHERE node_id = ?",
                (rs, i) -> new HardState(rs.getLong("current_term"), rs.getString("voted_for")),
                nodeId);
        return rows.isEmpty() ? HardState.INITIAL : rows.get(0);
    }

    @Override
    public void saveHardState(HardState state) {
        jdbc.update("""
                INSERT INTO raft_hard_state (node_id, current_term, voted_for) VALUES (?, ?, ?)
                ON CONFLICT (node_id) DO UPDATE
                SET current_term = EXCLUDED.current_term, voted_for = EXCLUDED.voted_for""",
                nodeId, state.currentTerm(), state.votedFor());
    }

    @Override
    public List<LogEntry> loadLog() {
        return jdbc.query(
                "SELECT log_index, term, command FROM raft_log WHERE node_id = ? ORDER BY log_index",
                (rs, i) -> new LogEntry(rs.getLong("log_index"), rs.getLong("term"), rs.getBytes("command")),
                nodeId);
    }

    @Override
    public void appendEntries(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(
                "INSERT INTO raft_log (node_id, log_index, term, command) VALUES (?, ?, ?, ?)",
                entries, entries.size(),
                (ps, e) -> {
                    ps.setString(1, nodeId);
                    ps.setLong(2, e.index());
                    ps.setLong(3, e.term());
                    ps.setBytes(4, e.command() == null ? new byte[0] : e.command());
                });
    }

    @Override
    public void deleteEntriesFrom(long fromIndex) {
        jdbc.update("DELETE FROM raft_log WHERE node_id = ? AND log_index >= ?", nodeId, fromIndex);
    }
}
