package com.raftkv.raft;

import com.google.protobuf.ByteString;
import com.raftkv.raft.rpc.LogEntryProto;

/**
 * One slot in the replicated log.
 *
 * @param index   1-based position in the log (index 0 is an implicit empty sentinel)
 * @param term    term of the leader that created the entry
 * @param command opaque bytes handed to the state machine once the entry is committed;
 *                an empty command is a no-op (leaders append one when elected)
 */
public record LogEntry(long index, long term, byte[] command) {

    public boolean isNoOp() {
        return command == null || command.length == 0;
    }

    public LogEntryProto toProto() {
        return LogEntryProto.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setCommand(ByteString.copyFrom(command == null ? new byte[0] : command))
                .build();
    }

    public static LogEntry fromProto(LogEntryProto proto) {
        return new LogEntry(proto.getIndex(), proto.getTerm(), proto.getCommand().toByteArray());
    }
}
