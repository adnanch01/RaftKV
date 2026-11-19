package com.raftkv.raft.transport;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.raftkv.raft.rpc.AppendEntriesRequest;
import com.raftkv.raft.rpc.AppendEntriesResponse;
import com.raftkv.raft.rpc.RaftServiceGrpc;
import com.raftkv.raft.rpc.RequestVoteRequest;
import com.raftkv.raft.rpc.RequestVoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sends Raft RPCs over gRPC: one long-lived HTTP/2 channel per peer, with a deadline on
 * every call.
 *
 * <p>The deadline matters. Without it, a call to a hung peer could wait forever. A failed
 * or timed-out call needs no retry logic here, because Raft retries by design: the leader
 * re-sends AppendEntries on every heartbeat, and candidates simply time out and start a
 * new election.
 */
public class GrpcRaftTransport implements RaftTransport {

    private final Map<String, ManagedChannel> channels = new HashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceFutureStub> stubs = new HashMap<>();
    private final Duration rpcTimeout;
    private final NetworkSimulator network;

    /** @param peerAddresses peer id to "host:port" of its gRPC server */
    public GrpcRaftTransport(Map<String, String> peerAddresses, Duration rpcTimeout, NetworkSimulator network) {
        this.rpcTimeout = rpcTimeout;
        this.network = network;
        peerAddresses.forEach((peerId, address) -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
            channels.put(peerId, channel);
            stubs.put(peerId, RaftServiceGrpc.newFutureStub(channel));
        });
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request) {
        if (network.isIsolated()) {
            return isolatedFailure();
        }
        return toCompletable(stub(peerId).requestVote(request));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String peerId, AppendEntriesRequest request) {
        if (network.isIsolated()) {
            return isolatedFailure();
        }
        return toCompletable(stub(peerId).appendEntries(request));
    }

    private RaftServiceGrpc.RaftServiceFutureStub stub(String peerId) {
        RaftServiceGrpc.RaftServiceFutureStub stub = stubs.get(peerId);
        if (stub == null) {
            throw new IllegalArgumentException("Unknown peer " + peerId);
        }
        return stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static <T> CompletableFuture<T> isolatedFailure() {
        return CompletableFuture.failedFuture(
                Status.UNAVAILABLE.withDescription("node is isolated").asRuntimeException());
    }

    private static <T> CompletableFuture<T> toCompletable(ListenableFuture<T> listenable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Futures.addCallback(listenable, new FutureCallback<>() {
            @Override
            public void onSuccess(T result) {
                future.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
