package com.mq.replication;

import com.mq.cluster.BrokerRegistry;
import com.mq.cluster.PartitionMetadata;
import com.mq.dto.request.ReplicateRequest;
import com.mq.dto.response.ReplicateResponse;
import com.mq.model.BrokerInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Locale.filter;


@Slf4j
@RequiredArgsConstructor
@Service
public class ReplicationService {

    private final BrokerRegistry brokerRegistry;
    private final PartitionMetadata partitionMetadata;
    private final ISRTracker isrTracker;
    private final RestTemplate restTemplate;

    @Value("${broker.id:broker-1}")
    private String currentBrokerId;

    /**
     * Replication factor
     * 1 leader + 2 followers = 3
     */
    @Value("${replication.factor:3}")
    private int replicationFactor;

    /**
     * Timeout for each replication HTTP calls
     * if follower doesn't respond within this time, it will be marked as failed and removed from ISR
     */
    @Value("${replication.timeout.ms:5000}")
    private int replicationTimeoutMs;


    /**
     * Replicate a message from this leader to all followers
     * <p>
     * this is gonna called by producer controller after writing to local log
     * <p>
     * the producer only get success response after the majority of ISR have the message replicated successfully
     *
     * @param topicName
     * @param partitionIndex
     * @param payload        raw byte message
     * @param offset         this is assign by leader
     * @return true if the majority of followers successfully replicated the message, false otherwise
     */
    public boolean replicationToFollowers(String topicName,
                                          int partitionIndex,
                                          byte[] payload,
                                          long offset) {

        // Check if THIS broker is the leader for this partition
        Optional<BrokerInfo> leaderOpt =
                partitionMetadata.getLeader(topicName, partitionIndex);

        if (leaderOpt.isEmpty()) {
            log.debug("No leader assigned for {}-{}, skipping replication",
                    topicName, partitionIndex);
            return true;
        }

        String leaderId = leaderOpt.get().getBrokerId();

        // CRITICAL CHECK: Only replicate if WE are the leader
        // If we are not the leader, skip replication entirely
        // (the actual leader will handle it)
        if (!leaderId.equals(currentBrokerId)) {
            log.debug("This broker ({}) is not the leader ({}) for {}-{}. " +
                            "Skipping replication.",
                    currentBrokerId, leaderId, topicName, partitionIndex);
            return true;
        }

        // We ARE the leader — replicate to all OTHER brokers
        List<BrokerInfo> followers = brokerRegistry.getAllBrokers()
                .stream()
                .filter(b -> !b.getBrokerId().equals(currentBrokerId))
                .collect(Collectors.toList());

        if (followers.isEmpty()) {
            log.debug("No followers available, single-node mode");
            return true;
        }

        // Build replication request
        ReplicateRequest request = new ReplicateRequest(
                topicName,
                partitionIndex,
                Base64.getEncoder().encodeToString(payload),
                offset
        );

        // Send to all followers in parallel
        List<CompletableFuture<Boolean>> futures = followers.stream()
                .map(follower -> replicateToOne(follower, request))
                .collect(Collectors.toList());

        int successCount = 1; // leader itself = 1

        for (CompletableFuture<Boolean> future : futures) {
            try {
                boolean success = future.get(
                        replicationTimeoutMs, TimeUnit.MILLISECONDS);
                if (success) successCount++;
            } catch (TimeoutException e) {
                log.warn("Replication timeout for {}-{}",
                        topicName, partitionIndex);
            } catch (Exception e) {
                log.error("Replication error: {}", e.getMessage());
            }
        }

        int majority = (replicationFactor / 2) + 1;
        boolean majorityAchieved = successCount >= majority;

        if (!majorityAchieved) {
            log.error("Failed majority replication for {}-{} offset {}. " +
                            "Got {}/{} acks",
                    topicName, partitionIndex, offset,
                    successCount, replicationFactor);
        }

        return majorityAchieved;
    }

    /**
     * Replicate to single follower async
     * we use CompletableFuture = for parallel replication
     */
    private CompletableFuture<Boolean> replicateToOne(BrokerInfo follower, ReplicateRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {

                String url = follower.baseUrl() + "/api/v1/replication/replicate";

                ReplicateResponse response = restTemplate.postForObject(url, request, ReplicateResponse.class);

                if (response != null && response.isSuccess()) {
                    isrTracker.recordReplication(
                            request.getTopicName(),
                            request.getPartitionIdx(),
                            follower.getBrokerId(),
                            request.getOffset()
                    );
                    return true;
                }

                return false;

            } catch (Exception e) {
                log.warn("Failed to replicate to follower {} for {}-{} at offset {}, error: {}",
                        follower.getBrokerId(), request.getTopicName(), request.getPartitionIdx(), request.getOffset(), e.getMessage());
                return false;
            }
        });
    }


    /**
     * get follower for a partition
     * <p>
     * 1. get all registered brokers
     * 2. remove leader
     * 3. remaining are follower
     */
    private List<BrokerInfo> getFollowers(String topicName, int partitionIdx) {
        Optional<BrokerInfo> leader = partitionMetadata.getLeader(topicName, partitionIdx);
        if (leader.isEmpty()) return Collections.emptyList();

        String leaderId = leader.get().getBrokerId();

        return brokerRegistry.getAllBrokers().stream()
                .filter(b -> !b.getBrokerId().equals(leaderId))
                .collect(Collectors.toList());
    }

    /**
     * Get replication metrics for partition
     */
    public Map<String, Object> getReplicationMetrics(String topicName, int partitionIdx, long leaderOffset) {
        List<BrokerInfo> followers = getFollowers(topicName, partitionIdx);
        Set<String> followerIds = followers.stream()
                .map(BrokerInfo::getBrokerId)
                .collect(Collectors.toSet());

        Set<String> isrFollowers = isrTracker.getISR(topicName, partitionIdx, followerIds);

        Map<String, Object> metrics = new HashMap<>();
        metrics.put("leaderOffset", leaderOffset);
        metrics.put("ISR", isrFollowers);
        metrics.put("followers", isrTracker.getReplicationStatus(
                topicName, partitionIdx, followerIds, leaderOffset));

        return metrics;
    }

}
