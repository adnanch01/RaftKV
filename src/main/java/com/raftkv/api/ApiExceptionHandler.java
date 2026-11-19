package com.raftkv.api;

import com.raftkv.config.RaftProperties;
import com.raftkv.raft.NotLeaderException;
import com.raftkv.raft.ProposalDroppedException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/** Maps Raft-level failures to meaningful HTTP responses. */
@RestControllerAdvice
public class ApiExceptionHandler {

    private final RaftProperties props;

    public ApiExceptionHandler(RaftProperties props) {
        this.props = props;
    }

    /**
     * A follower got a request only the leader can serve. If the leader is known, return
     * 307 Temporary Redirect. 307, unlike 302, tells clients to repeat the same method and
     * body, so {@code curl -L} transparently retries a PUT on the leader. During an
     * election no leader is known, so return 503 and let the client retry shortly.
     */
    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<Map<String, Object>> notLeader(NotLeaderException e, HttpServletRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "NOT_LEADER");
        body.put("message", e.getMessage());
        body.put("leaderId", e.getLeaderId());
        if (e.getLeaderId() == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
        }
        String query = request.getQueryString() == null ? "" : "?" + request.getQueryString();
        String location = props.member(e.getLeaderId()).httpAddress() + request.getRequestURI() + query;
        body.put("leaderAddress", location);
        return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                .header(HttpHeaders.LOCATION, location)
                .body(body);
    }

    @ExceptionHandler(ProposalDroppedException.class)
    public ResponseEntity<Map<String, Object>> dropped(ProposalDroppedException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "LEADERSHIP_CHANGED", "message", e.getMessage(), "retryable", true));
    }

    @ExceptionHandler(KvController.CommitTimeoutException.class)
    public ResponseEntity<Map<String, Object>> timeout(KvController.CommitTimeoutException e) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(Map.of("error", "COMMIT_TIMEOUT", "message", e.getMessage()));
    }
}
