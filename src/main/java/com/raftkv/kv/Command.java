package com.raftkv.kv;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A key-value command, as stored in the Raft log.
 *
 * <p>Raft treats commands as opaque bytes, so this is serialized to JSON before it is
 * proposed. GET goes through the log too: when the read gets applied at its log
 * position, it reflects every write committed before it. That makes it linearizable.
 */
public record Command(Type type, String key, String value) {

    public enum Type { PUT, DELETE, GET }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Command put(String key, String value) {
        return new Command(Type.PUT, key, value);
    }

    public static Command delete(String key) {
        return new Command(Type.DELETE, key, null);
    }

    public static Command get(String key) {
        return new Command(Type.GET, key, null);
    }

    public byte[] encode() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Command decode(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, Command.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
