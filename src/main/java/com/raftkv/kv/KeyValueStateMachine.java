package com.raftkv.kv;

import com.raftkv.raft.StateMachine;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The replicated application: a plain in-memory map.
 *
 * <p>This class knows nothing about Raft. It only applies committed commands in order.
 * Because {@link #apply} is deterministic, all five nodes hold identical maps once they
 * have applied the same log prefix.
 *
 * <p>Only {@code RaftNode} calls {@code apply}, from one thread at a time. The map is
 * concurrent because the REST layer may read it directly for "stale" reads.
 */
public class KeyValueStateMachine implements StateMachine {

    private final Map<String, String> data = new ConcurrentHashMap<>();

    @Override
    public Object apply(long index, byte[] commandBytes) {
        Command command = Command.decode(commandBytes);
        return switch (command.type()) {
            case PUT -> {
                String previous = data.put(command.key(), command.value());
                yield new KvResult(command.key(), command.value(), previous != null, index);
            }
            case DELETE -> {
                String removed = data.remove(command.key());
                yield new KvResult(command.key(), removed, removed != null, index);
            }
            case GET -> {
                String value = data.get(command.key());
                yield new KvResult(command.key(), value, value != null, index);
            }
        };
    }

    /** Local, possibly stale read. No Raft round trip. */
    public String getLocal(String key) {
        return data.get(key);
    }

    public Map<String, String> snapshot() {
        return new TreeMap<>(data);
    }
}
