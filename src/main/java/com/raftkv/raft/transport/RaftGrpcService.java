package com.raftkv.raft.transport;

import com.raftkv.raft.RaftNode;
import com.raftkv.raft.rpc.AppendEntriesRequest;
import com.raftkv.raft.rpc.AppendEntriesResponse;
import com.raftkv.raft.rpc.RaftServiceGrpc;
import com.raftkv.raft.rpc.RequestVoteRequest;
import com.raftkv.raft.rpc.RequestVoteResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** Server side of the Raft RPCs: unwraps each gRPC call and hands it to the {@link RaftNode}. */
public class RaftGrpcService extends RaftServiceGrpc.RaftServiceImplBase {

    private final RaftNode node;
    private final NetworkSimulator network;

    public RaftGrpcService(RaftNode node, NetworkSimulator network) {
        this.node = node;
        this.network = network;
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
        if (rejectIfIsolated(observer)) {
            return;
        }
        observer.onNext(node.handleRequestVote(request));
        observer.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
        if (rejectIfIsolated(observer)) {
            return;
        }
        observer.onNext(node.handleAppendEntries(request));
        observer.onCompleted();
    }

    private boolean rejectIfIsolated(StreamObserver<?> observer) {
        if (network.isIsolated()) {
            observer.onError(Status.UNAVAILABLE.withDescription("node is isolated").asRuntimeException());
            return true;
        }
        return false;
    }
}
