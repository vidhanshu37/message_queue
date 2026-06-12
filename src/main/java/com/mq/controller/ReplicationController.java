package com.mq.controller;

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

    @Value("${broker.id:broker-1}")
    private String currentBrokerId;

    /**
     * Received replication message from leader
     * <p>
     * this is gonna call by leader's replicationService
     * followers must do :
     * 1. decode payload
     * 2. write to its own log at exact same offset
     * 3. return success/failure
     */
    @PostMapping("/replicate")
    public ResponseEntity<ReplicateResponse> replicate(@RequestBody ReplicateRequest request) {
        try {
            // step1 - decode payload (base64 -> byte)
            byte[] payload = Base64.getDecoder().decode(request.getPayloadBase64());

            // step2 - write to log at the same offset
            logManager.initPartition(
                    request.getTopicName(),
                    request.getPartitionIdx()
            );

            logManager.appendAtOffset(
                    request.getTopicName(),
                    request.getPartitionIdx(),
                    payload,
                    request.getOffset()
            );

            log.debug("Replicated {}-{} offset {} on follower {}",
                    request.getTopicName(), request.getPartitionIdx(), request.getOffset(), currentBrokerId);

            return ResponseEntity.ok(new ReplicateResponse(
                    true, currentBrokerId, request.getOffset(), "Replicated successfully"
            ));

        } catch (IOException e) {
            log.error("Replication failed on follower {}: {}",
                    currentBrokerId, e.getMessage());

            return ResponseEntity.ok(new ReplicateResponse(
                    false,
                    currentBrokerId,
                    -1,
                    "Replication failed: " + e.getMessage()
            ));
        }
    }

    /**
     * Get replication health
     */
    @GetMapping("/health")
    public ResponseEntity<?> replicationHealth() {
        return ResponseEntity.ok(Map.of(
                "brokerId", currentBrokerId,
                "status", "UP",
                "timestamp", Instant.now()
        ));
    }


}
