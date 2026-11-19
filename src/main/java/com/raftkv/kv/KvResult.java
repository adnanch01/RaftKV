package com.raftkv.kv;

/**
 * The outcome of applying a {@link Command}.
 *
 * @param value for GET the current value, for PUT the new value, for DELETE the removed value
 * @param found whether the key existed before the command ran
 * @param index the log index at which the command was applied
 */
public record KvResult(String key, String value, boolean found, long index) {
}
