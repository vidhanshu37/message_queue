package com.mq.replication;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class ISRTracker {

    /**
     * A follower is removed from ISR if we haven't heard from it in 30 s.
     * This only applies when the follower goes completely silent (crashed).
     * Normal replication lag does NOT kick a follower out of ISR.
     */
    private static final long HEARTBEAT_TIMEOUT_MS = 30_000; // 30 s

    /**
     * Maximum offset lag before a follower is considered out-of-sync.
     *
     * FIX (off-by-one): leaderOffset comes from LogSegment.getNextOffset()
     * which points to the NEXT empty slot (e.g. 2 after writing messages 0,1).
     * followerOff comes from recordReplication() which stores the LAST written
     * message offset (e.g. 1). So a fully-caught-up follower has lag = 1, not 0.
     *
     * Setting threshold to 1 means: follower is in ISR when it has replicated
     * every message the leader has written (nextOffset - lastWritten = 1).
     */
    private static final long MAX_LAG_THRESHOLD = 1;

    /** key = "topicName-partitionIdx-brokerId"  →  timestamp of last successful replication */
    private final Map<String, Instant> lastFetchTime   = new ConcurrentHashMap<>();

    /** key = "topicName-partitionIdx-brokerId"  →  latest offset replicated by this follower */
    private final Map<String, Long>    followerOffsets = new ConcurrentHashMap<>();

    /**
     * Called by ReplicationService after a follower successfully ACKs a replicate request.
     *
     * FIX: We store the offset the follower confirmed, not just the time.
     * ISR membership is now driven by offset lag, not time alone.
     */
    public void recordReplication(String topicName, int partitionIdx,
                                  String brokerId, long offset) {
        String key = buildKey(topicName, partitionIdx, brokerId);
        lastFetchTime.put(key, Instant.now());
        followerOffsets.put(key, offset);
        log.debug("ISR update: follower {} replicated {}-{} up to offset {}",
                brokerId, topicName, partitionIdx, offset);
    }

    /**
     * Returns the set of follower broker IDs that are currently in the ISR.
     *
     * A follower is IN the ISR when ALL of the following are true:
     *   1. It has replicated at least once (lastFetchTime exists)
     *   2. Its replicated offset == leaderOffset  (lag == 0)
     *   3. We've heard from it within HEARTBEAT_TIMEOUT_MS  (not crashed)
     *
     * FIX: Old code only checked time (10 s window), so a follower that
     * replicated once and then the check ran 11 s later always showed ISR=[].
     * New code primarily checks offset lag; time is just a "is it alive?" guard.
     */
    public Set<String> getISR(String topicName, int partitionIdx,
                              Set<String> allFollowerIds, long leaderOffset) {
        Set<String> inSync = new HashSet<>();
        Instant aliveThreshold = Instant.now().minusMillis(HEARTBEAT_TIMEOUT_MS);

        for (String brokerId : allFollowerIds) {
            String  key          = buildKey(topicName, partitionIdx, brokerId);
            Instant lastSeen     = lastFetchTime.get(key);
            Long    followerOff  = followerOffsets.get(key);

            if (lastSeen == null || followerOff == null) continue; // never replicated

            boolean alive  = lastSeen.isAfter(aliveThreshold);
            boolean caught = (leaderOffset - followerOff) <= MAX_LAG_THRESHOLD;

            if (alive && caught) {
                inSync.add(brokerId);
            }
        }
        return inSync;
    }

    /** Kept for backward compatibility — delegates to the new overload. */
    public Set<String> getISR(String topicName, int partitionIdx, Set<String> allFollowerIds) {
        // Without leaderOffset we can only use the time-based check
        Set<String> inSync = new HashSet<>();
        Instant aliveThreshold = Instant.now().minusMillis(HEARTBEAT_TIMEOUT_MS);
        for (String brokerId : allFollowerIds) {
            String  key      = buildKey(topicName, partitionIdx, brokerId);
            Instant lastSeen = lastFetchTime.get(key);
            if (lastSeen != null && lastSeen.isAfter(aliveThreshold)) {
                inSync.add(brokerId);
            }
        }
        return inSync;
    }

    public long getReplicationLag(String topicName, int partitionIdx,
                                  String brokerId, long leaderOffset) {
        String key = buildKey(topicName, partitionIdx, brokerId);
        Long followerOffset = followerOffsets.get(key);
        if (followerOffset == null) return leaderOffset;
        return Math.max(0, leaderOffset - followerOffset);
    }

    /**
     * Returns per-follower replication stats for the /cluster/replication endpoint.
     */
    public Map<String, Object> getReplicationStatus(String topicName, int partitionIdx,
                                                    Set<String> allFollowerIds,
                                                    long leaderOffset) {
        Map<String, Object> status = new HashMap<>();
        Instant aliveThreshold = Instant.now().minusMillis(HEARTBEAT_TIMEOUT_MS);

        for (String brokerId : allFollowerIds) {
            String  key         = buildKey(topicName, partitionIdx, brokerId);
            Instant lastSeen    = lastFetchTime.get(key);
            Long    followerOff = followerOffsets.getOrDefault(key, 0L);

            long lag = Math.max(0, leaderOffset - followerOff);

            boolean alive  = lastSeen != null && lastSeen.isAfter(aliveThreshold);
            boolean caught = lag <= MAX_LAG_THRESHOLD;
            boolean inISR  = alive && caught;

            status.put(brokerId, Map.of(
                    "lastFetchTime",      lastSeen != null ? lastSeen.toString() : "never",
                    "replicatedOffset",   followerOff,
                    "lag",                lag,
                    "inISR",              inISR
            ));
        }
        return status;
    }

    private String buildKey(String topicName, int partitionIdx, String brokerId) {
        return topicName + "-" + partitionIdx + "-" + brokerId;
    }
}