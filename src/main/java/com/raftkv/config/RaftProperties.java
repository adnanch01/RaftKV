package com.raftkv.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Settings bound from the {@code raft.*} properties in application.yml.
 *
 * <p>Every node gets the same {@code cluster} list. Only {@code nodeId} differs, and it
 * tells each node which entry in the list is itself.
 */
@ConfigurationProperties(prefix = "raft")
public record RaftProperties(
        String nodeId,
        int grpcPort,
        List<Member> cluster,
        Duration electionTimeoutMin,
        Duration electionTimeoutMax,
        Duration heartbeatInterval,
        Duration rpcTimeout,
        Duration clientTimeout,
        Storage storage) {

    /**
     * @param grpcAddress host:port that peers use for Raft RPCs
     * @param httpAddress base URL that clients are redirected to when this member leads
     */
    public record Member(String id, String grpcAddress, String httpAddress) {
    }

    /** @param type "memory" (non-durable, for local runs) or "postgres" */
    public record Storage(String type, String url, String username, String password) {
    }

    public Member self() {
        return member(nodeId);
    }

    public Member member(String id) {
        return cluster.stream()
                .filter(m -> m.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Node '" + id + "' is not in raft.cluster"));
    }

    public List<Member> peers() {
        return cluster.stream().filter(m -> !m.id().equals(nodeId)).toList();
    }
}
