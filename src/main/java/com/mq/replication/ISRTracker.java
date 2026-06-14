package com.mq.replication;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// ISR = in-sync replication with leader n follower
@Slf4j
@Component
public class ISRTracker {

    /**
     * 10 seconds matches
     */
    private static final long LAG_THRESHOLD_MS = 30_000; // 10 sec

    /**
     * Tracks the last time each follower fetched from leader
     *
     * key = "topicName-partitionIdx-brokerId"
     * value = timestamp of last successful replication
     */
    private final Map<String, Instant> lastFetchTime = new ConcurrentHashMap<>();

    /**
     * Tracks latest offset that each follower has replicated successfully
     * key = "topicName-partitionIdx-brokerId"
     * value = latest offset replicated by this follower
     */
    private final Map<String, Long> followerOffsets = new ConcurrentHashMap<>();

    /**
     * Called by follower after successful fetch from leader
     * Updates last fetch time and replicated offset for this follower
     */
    public void recordReplication(String topicName, int partitionIdx,
                                  String brokerId, long offset) {
        String key = buildKey(topicName, partitionIdx, brokerId);
        lastFetchTime.put(key, Instant.now());
        followerOffsets.put(key, offset);

        log.debug("Follower {} replicated {}-{} up to offset {}",
                brokerId, topicName, partitionIdx, offset);
    }

    /**
     * Get all brokers currently in-sync for a partition
     *
     * - A broker is considered in-sync
     *      if its last fetch time is within the LAG_THRESHOLD_MS
     * - This method is called by leader to determine
     *      which followers are in-sync and can be part of ISR set
     */
    public Set<String> getISR(String topicName, int partitionIdx, Set<String> allFollowerIds) {
        Set<String> inSync = new HashSet<>();
        Instant threshold = Instant.now().minusMillis(LAG_THRESHOLD_MS);

        for(String brokerId : allFollowerIds) {
            String key = buildKey(topicName, partitionIdx, brokerId);
            Instant lastSeen = lastFetchTime.get(key);

            if(lastSeen != null && lastSeen.isAfter(threshold)) {
                inSync.add(brokerId);
            }
        }

        return inSync;
    }

    /**
     * how far behind is follower ?
     * lag = leaderOffset - followerOffset
     */
    public long getReplicationLag(String topicName, int partitionIdx, String brokerId, long leaderOffset) {
        String key = buildKey(topicName, partitionIdx, brokerId);
        Long followerOffset = followerOffsets.get(key);

        if(followerOffset == null) {
            return leaderOffset; // never replicated, lag is full
        }

        return Math.max(0, leaderOffset - followerOffset);
    }

    /**
     * Get replication status for all follower of a partition
     */
    public Map<String, Object> getReplicationStatus(String topicName, int partitionIdx, Set<String> allFollowerIds, long leaderOffset) {
        Map<String, Object> status = new HashMap<>();

        for(String brokerId : allFollowerIds) {
            String key = buildKey(topicName, partitionIdx, brokerId);
            Instant lastSeen = lastFetchTime.get(key);
            Long followerOffset = followerOffsets.getOrDefault(key, 0L);

            long lag = Math.max(0, leaderOffset - followerOffset);

            status.put(brokerId, Map.of(
                    "lastFetchTime", lastSeen != null
                            ? lastSeen.toString() : "never",
                    "replicatedOffset", followerOffset,
                    "lag", lag,
                    "inISR", lastSeen != null &&
                            lastSeen.isAfter(
                                    Instant.now().minusMillis(LAG_THRESHOLD_MS)) && lag == 0
            ));
        }

        return status;
    }


    private String buildKey(String topicName, int partitionIdx, String brokerId) {
        return topicName + "-" + partitionIdx + "-" + brokerId;
    }


}
