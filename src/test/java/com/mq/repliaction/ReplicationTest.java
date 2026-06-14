package com.mq.repliaction;

import com.mq.replication.ISRTracker;
import com.mq.storage.LogManager;
import com.mq.storage.MessageRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ReplicationTest {
    @TempDir
    Path leaderDir;
    @TempDir
    Path followerDir;

    @Test
    void leaderAndFollower_haveIdenticalLogs() throws IOException {
        // setup leader log
        LogManager leader = new LogManager(leaderDir.toString());
        leader.initPartition("orders", 0);

        // setup follower log
        LogManager follower = new LogManager(followerDir.toString());
        follower.initPartition("orders", 0);

        // write 5 messages to leader
        byte[][] payload = {
                "pizza order".getBytes(),
                "burger order".getBytes(),
                "pasta order".getBytes(),
                "sushi order".getBytes(),
                "taco order".getBytes()
        };

        for(int i=0; i<payload.length; i++) {
            // leader assigns offset
            long offset = leader.append("orders", 0, payload[i]);

            // follower replicates with same offset
            follower.appendAtOffset("orders", 0, payload[i], offset);
        }

        // read from both
        List<MessageRecord> leaderRecords = leader.read("orders", 0, 0, 1024 * 1024);
        List<MessageRecord> followerRecords = follower.read("orders", 0, 0, 1024 * 1024);

        // Verify identical
        assertThat(leaderRecords).hasSize(5);
        assertThat(followerRecords).hasSize(5);

        for(int i=0; i<5; i++) {
            assertThat(followerRecords.get(i).getOffset())
                    .isEqualTo(leaderRecords.get(i).getOffset());
            assertThat(followerRecords.get(i).getPayload())
                    .isEqualTo(leaderRecords.get(i).getPayload());
        }
    }

    /**
     * Test: ISR tracking correctly
     *
     */
    @Test
    void isTracker_correctlyTracksFollowers() throws Exception {
        ISRTracker tracker = new ISRTracker();

        // Record replication from broker-2
        tracker.recordReplication("orders", 0, "broker-2", 42);

        Set<String> isr = tracker.getISR("orders", 0, Set.of("broker-2", "broker-3"));

        assertThat(isr).contains("broker-2");
        // broker-3 never replicated → not in ISR
        assertThat(isr).doesNotContain("broker-3");
    }

    /**
     * Test: Lag calculation is correct.
     */
    @Test
    void isrTracker_calculatesLagCorrectly() {
        ISRTracker tracker = new ISRTracker();

        // broker-2 has replicated up to offset 90
        tracker.recordReplication("orders", 0, "broker-2", 90);

        // Leader is at offset 100
        long lag = tracker.getReplicationLag(
                "orders", 0, "broker-2", 100);

        assertThat(lag).isEqualTo(10);
    }

    /**
     * Test: Follower Recovery - if follower restarts,
     * it can receive new replication and catch up
     */
    @Test
    void follower_canCatchUpAfterRestart() throws IOException {
        LogManager follower = new LogManager(followerDir.toString());

        follower.initPartition("orders", 0);

        // write some message
        for(int i=0; i<5; i++) {
            follower.appendAtOffset("orders", 0, ("orders-" + i).getBytes(), i);
        }

        // Simulate restart by creating new LogManager instance
        LogManager restart = new LogManager(followerDir.toString());
        restart.initPartition("orders", 0);

        // Verify it can read existing messages
        List<MessageRecord> records = restart.read("orders", 0, 0, 1024 * 1024);

        assertThat(records).hasSize(5);
        assertThat(records.get(4).getOffset()).isEqualTo(4L);

        // can receive new replication after restart
        restart.appendAtOffset("orders", 0, "new-orders".getBytes(), 5);

        records = restart.read("orders", 0, 5, 1024 * 1024);

        assertThat(records).hasSize(1);
        assertThat(new String(records.get(0).getPayload())).isEqualTo("new-orders");
    }
}
