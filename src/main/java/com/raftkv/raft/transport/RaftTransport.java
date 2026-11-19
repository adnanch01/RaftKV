package com.raftkv.raft.transport;

import com.raftkv.raft.rpc.AppendEntriesRequest;
import com.raftkv.raft.rpc.AppendEntriesResponse;
import com.raftkv.raft.rpc.RequestVoteRequest;
import com.raftkv.raft.rpc.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * How a node sends RPCs to its peers.
 *
 * <p>The Raft algorithm never depends on the transport. Production uses
 * {@link GrpcRaftTransport}; tests use an in-memory transport that can drop messages to
 * simulate crashes and network partitions. Calls are asynchronous, so a node can
 * contact all peers in parallel and never blocks while holding its lock.
 */
public interface RaftTransport {

    CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(String peerId, AppendEntriesRequest request);

    default void close() {
    }
}
