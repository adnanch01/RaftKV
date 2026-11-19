package com.raftkv.config;

import com.raftkv.raft.transport.RaftGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

/** Starts and stops the gRPC server that peers use to reach this node. */
@Component
public class GrpcServerLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

    private final RaftProperties props;
    private final RaftGrpcService service;
    private Server server;

    public GrpcServerLifecycle(RaftProperties props, RaftGrpcService service) {
        this.props = props;
        this.service = service;
    }

    @Override
    public void start() {
        try {
            server = ServerBuilder.forPort(props.grpcPort()).addService(service).build().start();
            log.info("Raft gRPC server for node {} listening on port {}", props.nodeId(), props.grpcPort());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not start gRPC server on port " + props.grpcPort(), e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdown();
            try {
                server.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return server != null && !server.isShutdown();
    }
}
