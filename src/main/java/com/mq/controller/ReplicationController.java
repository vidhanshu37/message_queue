package com.mq.controller;

import com.mq.cluster.BrokerRegistry;
import com.mq.cluster.PartitionMetadata;
import com.mq.dto.request.ReplicateRequest;
import com.mq.dto.response.ReplicateResponse;
import com.mq.storage.LogManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/replication")
@RequiredArgsConstructor
public class ReplicationController {

    private final LogManager logManager;
    private final PartitionMetadata partitionMetadata;
    private final BrokerRegistry brokerRegistry;

    @Value("${broker.id:broker-1}")
    private String currentBrokerId;

    /**
     * Receive a replicated message from the Raft leader.
     *
     * Steps:
     *  1. Decode Base64 payload
     *  2. Init the partition log if not already done (idempotent)
     *  3. Write to local log at the exact same offset the leader assigned
     *  4. Return success/failure
     */
    @PostMapping("/replicate")
    public ResponseEntity<ReplicateResponse> replicate(@RequestBody ReplicateRequest request) {
        try {
            // Step 1 – decode payload
            byte[] payload = Base64.getDecoder().decode(request.getPayloadBase64());

            // Step 2 – make sure partition log exists (idempotent)
            logManager.initPartition(request.getTopicName(), request.getPartitionIdx());

            // Step 3 – write at the leader-assigned offset
            logManager.appendAtOffset(
                    request.getTopicName(),
                    request.getPartitionIdx(),
                    payload,
                    request.getOffset()
            );

            // FIX: update this follower's partition metadata so it knows
            // who the leader is.  The sender of the replicate request IS the
            // current leader for this partition.
            if (request.getLeaderId() != null) {
                brokerRegistry.getAllBrokers().stream()
                        .filter(b -> b.getBrokerId().equals(request.getLeaderId()))
                        .findFirst()
                        .ifPresent(leaderInfo ->
                                partitionMetadata.assignLeader(
                                        request.getTopicName(),
                                        request.getPartitionIdx(),
                                        leaderInfo));
            }

            log.debug("Replicated {}-{} offset {} on follower {}",
                    request.getTopicName(), request.getPartitionIdx(),
                    request.getOffset(), currentBrokerId);

            return ResponseEntity.ok(new ReplicateResponse(
                    true, currentBrokerId, request.getOffset(), "Replicated successfully"));

        } catch (IOException e) {
            log.error("Replication failed on follower {}: {}", currentBrokerId, e.getMessage());
            return ResponseEntity.ok(new ReplicateResponse(
                    false, currentBrokerId, -1, "Replication failed: " + e.getMessage()));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<?> replicationHealth() {
        return ResponseEntity.ok(Map.of(
                "brokerId", currentBrokerId,
                "status", "UP",
                "timestamp", Instant.now()
        ));
    }

    @PostMapping("/init-partition")
    public ResponseEntity<?> initPartition(@RequestBody Map<String, Object> request) {
        try {
            String topicName   = (String) request.get("topicName");
            int partitionIndex = (Integer) request.get("partitionIndex");

            logManager.initPartition(topicName, partitionIndex);
            log.info("Partition {}-{} initialized on follower {}", topicName, partitionIndex, currentBrokerId);

            return ResponseEntity.ok(Map.of(
                    "status", "initialized",
                    "topicName", topicName,
                    "partitionIndex", partitionIndex,
                    "brokerId", currentBrokerId
            ));
        } catch (IOException e) {
            log.error("Failed to init partition: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}