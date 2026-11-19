package com.raftkv.api;

import com.raftkv.config.RaftProperties;
import com.raftkv.kv.Command;
import com.raftkv.kv.KeyValueStateMachine;
import com.raftkv.kv.KvResult;
import com.raftkv.raft.RaftNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Client-facing key-value API.
 *
 * <p>Writes, and reads with consistency=linearizable, are proposed to Raft. The request
 * blocks until the command is committed by a majority and applied, then returns the
 * result. Sending one to a follower gets a 307 redirect to the leader (see
 * {@link ApiExceptionHandler}).
 */
@RestController
@RequestMapping("/kv")
public class KvController {

    private final RaftNode node;
    private final KeyValueStateMachine store;
    private final RaftProperties props;

    public KvController(RaftNode node, KeyValueStateMachine store, RaftProperties props) {
        this.node = node;
        this.store = store;
        this.props = props;
    }

    @PutMapping("/{key}")
    public KvResult put(@PathVariable String key, @RequestBody String value) throws Exception {
        return propose(Command.put(key, value));
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<KvResult> delete(@PathVariable String key) throws Exception {
        KvResult result = propose(Command.delete(key));
        return result.found() ? ResponseEntity.ok(result) : ResponseEntity.status(404).body(result);
    }

    /**
     * @param consistency "linearizable" (default) sends the read through the Raft log, so it
     *                    always reflects the latest committed write. "stale" reads this
     *                    node's local copy, which is fast and works on any node, but may lag
     *                    behind (or come from a partitioned ex-leader).
     */
    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable String key,
                                 @RequestParam(defaultValue = "linearizable") String consistency) throws Exception {
        if ("stale".equalsIgnoreCase(consistency)) {
            String value = store.getLocal(key);
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("key", key);
            body.put("value", value);
            body.put("found", value != null);
            body.put("servedBy", node.getNodeId());
            body.put("consistency", "stale");
            return value != null ? ResponseEntity.ok(body) : ResponseEntity.status(404).body(body);
        }
        KvResult result = propose(Command.get(key));
        return result.found() ? ResponseEntity.ok(result) : ResponseEntity.status(404).body(result);
    }

    /** This node's entire local map. Not linearizable; handy for watching replication. */
    @GetMapping
    public Map<String, String> all() {
        return store.snapshot();
    }

    private KvResult propose(Command command) throws Exception {
        try {
            Object result = node.submit(command.encode())
                    .get(props.clientTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return (KvResult) result;
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        } catch (TimeoutException e) {
            // The entry may still commit later. The client cannot tell, which is why real
            // systems attach a unique request id to each command and deduplicate retries.
            throw new CommitTimeoutException();
        }
    }

    static class CommitTimeoutException extends RuntimeException {
        CommitTimeoutException() {
            super("Command was not committed in time (no majority reachable?). It may still commit later.");
        }
    }
}
