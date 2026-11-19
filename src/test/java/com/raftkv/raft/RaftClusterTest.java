package com.raftkv.raft;

import com.raftkv.kv.Command;
import com.raftkv.kv.KvResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavioral tests for the Raft engine: a 5-node cluster in one JVM, with crashes and
 * network partitions injected through {@link LocalCluster}.
 */
class RaftClusterTest {

    private LocalCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static KvResult put(RaftNode leader, String key, String value) throws Exception {
        return (KvResult) leader.submit(Command.put(key, value).encode()).get(5, TimeUnit.SECONDS);
    }

    @Test
    void electsExactlyOneLeader() {
        cluster = new LocalCluster(5);
        String leader = cluster.awaitLeader();

        assertThat(cluster.leadersAmong(cluster.ids)).containsExactly(leader);
        long term = cluster.nodes.get(leader).status().currentTerm();
        assertThat(term).isPositive();
    }

    @Test
    void replicatesWritesToEveryNode() throws Exception {
        cluster = new LocalCluster(5);
        RaftNode leader = cluster.nodes.get(cluster.awaitLeader());

        for (int i = 0; i < 20; i++) {
            put(leader, "key" + i, "value" + i);
        }

        for (String id : cluster.ids) {
            LocalCluster.await(() -> cluster.valueOn(id, "key19").isPresent(), "replication to " + id);
            for (int i = 0; i < 20; i++) {
                assertThat(cluster.valueOn(id, "key" + i)).contains("value" + i);
            }
        }
    }

    @Test
    void followersRejectWritesAndPointToTheLeader() {
        cluster = new LocalCluster(5);
        String leader = cluster.awaitLeader();
        String follower = cluster.ids.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

        assertThatThrownBy(() -> cluster.nodes.get(follower).submit(Command.put("k", "v").encode()))
                .isInstanceOf(NotLeaderException.class)
                .extracting(e -> ((NotLeaderException) e).getLeaderId())
                .isEqualTo(leader);
    }

    @Test
    void survivesLeaderCrashWithoutLosingCommittedData() throws Exception {
        cluster = new LocalCluster(5);
        String oldLeader = cluster.awaitLeader();
        put(cluster.nodes.get(oldLeader), "color", "blue");

        cluster.crash(oldLeader);
        String newLeader = cluster.awaitLeader();

        assertThat(newLeader).isNotEqualTo(oldLeader);
        // Leader Completeness: the committed write survived the leader change.
        assertThat(cluster.nodes.get(newLeader).status().currentTerm())
                .isGreaterThan(cluster.nodes.get(oldLeader).status().currentTerm());
        KvResult read = (KvResult) cluster.nodes.get(newLeader)
                .submit(Command.get("color").encode()).get(5, TimeUnit.SECONDS);
        assertThat(read.value()).isEqualTo("blue");

        put(cluster.nodes.get(newLeader), "color", "green");
        assertThat(cluster.valueOn(newLeader, "color")).contains("green");
    }

    @Test
    void toleratesTwoFailuresButNotThree() throws Exception {
        cluster = new LocalCluster(5);
        String leader = cluster.awaitLeader();
        List<String> followers = new ArrayList<>(cluster.ids);
        followers.remove(leader);

        // Two followers down: 3 of 5 remain, still a majority.
        cluster.crash(followers.get(0));
        cluster.crash(followers.get(1));
        put(cluster.nodes.get(leader), "a", "1");

        // A third failure: 2 of 5 cannot form a majority, so nothing can commit.
        cluster.crash(followers.get(2));
        var future = cluster.nodes.get(leader).submit(Command.put("b", "2").encode());
        Thread.sleep(1000);
        assertThat(future).isNotDone();
        assertThat(cluster.valueOn(leader, "b")).isEmpty();

        // Bring one back: the majority returns and the pending write commits.
        cluster.restart(followers.get(2));
        assertThat(((KvResult) future.get(5, TimeUnit.SECONDS)).value()).isEqualTo("2");
    }

    @Test
    void minorityPartitionCannotCommitAndHealsCorrectly() throws Exception {
        cluster = new LocalCluster(5);
        String oldLeader = cluster.awaitLeader();
        List<String> others = new ArrayList<>(cluster.ids);
        others.remove(oldLeader);

        // The old leader is stuck with one follower on the minority side (2 of 5).
        List<String> minority = List.of(oldLeader, others.get(0));
        List<String> majority = others.subList(1, 4);
        cluster.partition(minority, majority);

        // The old leader still THINKS it leads, but it cannot commit anything.
        var doomed = cluster.nodes.get(oldLeader).submit(Command.put("x", "from-old-leader").encode());

        // The majority side elects a new leader in a higher term and keeps working.
        String newLeader = cluster.awaitLeader(majority);
        put(cluster.nodes.get(newLeader), "x", "from-new-leader");
        assertThat(doomed).isNotDone();

        // Heal: the old leader sees the higher term, steps down, and its uncommitted
        // entry is overwritten. The client gets an error, not a silent loss.
        cluster.heal();
        cluster.awaitLeader();
        assertThatThrownBy(() -> doomed.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ProposalDroppedException.class);

        for (String id : cluster.ids) {
            LocalCluster.await(() -> cluster.valueOn(id, "x").filter("from-new-leader"::equals).isPresent(),
                    "convergence on " + id);
        }
    }

    @Test
    void restartedNodeRecoversFromPersistentStateAndCatchesUp() throws Exception {
        cluster = new LocalCluster(5);
        String leader = cluster.awaitLeader();
        String victim = cluster.ids.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

        put(cluster.nodes.get(leader), "before", "crash");
        LocalCluster.await(() -> cluster.valueOn(victim, "before").isPresent(), "initial replication");
        long termBeforeCrash = cluster.nodes.get(victim).status().currentTerm();

        cluster.crash(victim);
        put(cluster.nodes.get(leader), "during", "downtime");

        cluster.restart(victim);
        // The restarted node reloaded term and log from its disk...
        assertThat(cluster.nodes.get(victim).status().currentTerm()).isGreaterThanOrEqualTo(termBeforeCrash);
        assertThat(cluster.nodes.get(victim).status().lastLogIndex()).isPositive();
        // ...then re-applied its log and received what it missed.
        LocalCluster.await(() -> cluster.valueOn(victim, "during").isPresent(), "catch-up after restart");
        assertThat(cluster.valueOn(victim, "before")).contains("crash");
    }

    @Test
    void logsMatchAcrossAllNodesAfterChurn() throws Exception {
        cluster = new LocalCluster(5);
        for (int round = 0; round < 3; round++) {
            String leader = cluster.awaitLeader();
            for (int i = 0; i < 5; i++) {
                put(cluster.nodes.get(leader), "r" + round + "-" + i, "v");
            }
            cluster.crash(leader);
            cluster.awaitLeader();
            cluster.restart(leader);
        }
        String leader = cluster.awaitLeader();
        put(cluster.nodes.get(leader), "final", "write");

        // Log Matching Property: once caught up, every node holds an identical log.
        long last = cluster.nodes.get(leader).status().lastLogIndex();
        for (String id : cluster.ids) {
            LocalCluster.await(() -> cluster.nodes.get(id).status().commitIndex() >= last, id + " catching up");
        }
        List<LogEntry> reference = cluster.nodes.get(leader).logEntries(1, Integer.MAX_VALUE);
        for (String id : cluster.ids) {
            List<LogEntry> other = cluster.nodes.get(id).logEntries(1, Integer.MAX_VALUE);
            assertThat(other).hasSameSizeAs(reference);
            for (int i = 0; i < reference.size(); i++) {
                assertThat(other.get(i).term()).isEqualTo(reference.get(i).term());
                assertThat(other.get(i).command()).isEqualTo(reference.get(i).command());
            }
        }
    }
}
