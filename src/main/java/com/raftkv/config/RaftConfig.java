package com.raftkv.config;

import com.raftkv.kv.KeyValueStateMachine;
import com.raftkv.raft.RaftNode;
import com.raftkv.raft.storage.InMemoryRaftStorage;
import com.raftkv.raft.storage.PostgresRaftStorage;
import com.raftkv.raft.storage.RaftStorage;
import com.raftkv.raft.transport.GrpcRaftTransport;
import com.raftkv.raft.transport.NetworkSimulator;
import com.raftkv.raft.transport.RaftGrpcService;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Wires one Raft node together: storage, transport, and state machine feed into
 * {@link RaftNode}, and the gRPC service exposes the node to its peers.
 */
@Configuration
@EnableConfigurationProperties(RaftProperties.class)
public class RaftConfig {

    private static final Logger log = LoggerFactory.getLogger(RaftConfig.class);

    @Bean
    public NetworkSimulator networkSimulator() {
        return new NetworkSimulator();
    }

    @Bean(destroyMethod = "")
    public RaftStorage raftStorage(RaftProperties props) {
        String type = props.storage() == null ? "memory" : props.storage().type();
        if ("postgres".equalsIgnoreCase(type)) {
            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl(props.storage().url());
            ds.setUsername(props.storage().username());
            ds.setPassword(props.storage().password());
            ds.setMaximumPoolSize(4);
            log.info("Using PostgreSQL storage at {}", props.storage().url());
            return new PostgresRaftStorage(new JdbcTemplate(ds), props.nodeId());
        }
        log.warn("Using IN-MEMORY storage: Raft state is lost when this process exits");
        return new InMemoryRaftStorage();
    }

    @Bean
    public KeyValueStateMachine keyValueStateMachine() {
        return new KeyValueStateMachine();
    }

    @Bean(destroyMethod = "close")
    public GrpcRaftTransport raftTransport(RaftProperties props, NetworkSimulator network) {
        Map<String, String> peerAddresses = new LinkedHashMap<>();
        props.peers().forEach(p -> peerAddresses.put(p.id(), p.grpcAddress()));
        return new GrpcRaftTransport(peerAddresses, props.rpcTimeout(), network);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public RaftNode raftNode(RaftProperties props, GrpcRaftTransport transport,
                             RaftStorage storage, KeyValueStateMachine stateMachine) {
        RaftNode.Config config = new RaftNode.Config(
                props.nodeId(),
                props.peers().stream().map(RaftProperties.Member::id).toList(),
                props.electionTimeoutMin(),
                props.electionTimeoutMax(),
                props.heartbeatInterval());
        return new RaftNode(config, transport, storage, stateMachine);
    }

    @Bean
    public RaftGrpcService raftGrpcService(RaftNode node, NetworkSimulator network) {
        return new RaftGrpcService(node, network);
    }
}
