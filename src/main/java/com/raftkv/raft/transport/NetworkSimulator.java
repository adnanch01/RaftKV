package com.raftkv.raft.transport;

/**
 * A learning tool: lets you cut a node off from the cluster at runtime (via
 * {@code POST /raft/admin/isolate}) without killing the process.
 *
 * <p>While isolated, the node drops every outgoing RPC and rejects every incoming one,
 * so it behaves as if its network cable were unplugged. Its REST API keeps working, so
 * you can watch its term climb as it keeps holding elections it cannot win.
 */
public class NetworkSimulator {

    private volatile boolean isolated;

    public boolean isIsolated() {
        return isolated;
    }

    public void setIsolated(boolean isolated) {
        this.isolated = isolated;
    }
}
