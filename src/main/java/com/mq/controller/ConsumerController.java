package com.mq.controller;

import com.mq.dto.response.ConsumeResponse;
import com.mq.service.ConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/consumer")
@RequiredArgsConstructor
public class ConsumerController {
    private final ConsumerService consumerService;

    /**
     * Poll messages from partition
     * <p>
     * returns :
     * 200 OK - messages were available with nextOffset
     * 204 No content - partition empty
     */
    @GetMapping("/consume")
    public ResponseEntity<ConsumeResponse> consume(
            @RequestParam String topic,
            @RequestParam String group,
            @RequestParam(defaultValue = "0") int partition,
            @RequestParam(defaultValue = "-1") long offset) {

        ConsumeResponse response = consumerService.consume(
                topic, group, partition, offset);

        // if response empty -> return 204 "No content" empty partition
        if (response.isEmpty()) {
            HttpHeaders headers = new HttpHeaders();
            headers.add("Retry-After", "1");

            return ResponseEntity.status(HttpStatus.NO_CONTENT)
                    .headers(headers)
                    .build();
        }

        return ResponseEntity.ok(response);
    }


    @GetMapping("/group-consume")
    public ResponseEntity<?> groupConsume(
            @RequestParam String topic,
            @RequestParam String groupId,
            @RequestParam String consumerId,
            @RequestParam int partition,
            @RequestParam int generation,
            @RequestParam(defaultValue = "-1") long offset) {
        log.info("Group consume: topic={} group={} consumer={} partition={} gen={} offset={}",
                topic, groupId, consumerId, partition, generation, offset);

        try {
            ConsumeResponse response = consumerService.consumeAsGroupMember(topic, groupId, consumerId, partition, generation, offset);

            if(response.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NO_CONTENT).body(response);
            }

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.warn("Partition ownership violation: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of(
                            "error", e.getMessage(),
                            "hint", "Call GET /api/v1/groups/" + groupId +
                                    "/assignment?consumerId=" + consumerId +
                                    " to see your assigned partitions"
                    ));
        } catch (IllegalStateException e) {
            // Stale generation OR group rebalancing OR not a member
            String msg = e.getMessage();
            boolean isGeneration = msg != null && msg.contains("generation");
            boolean isRebalancing = msg != null && msg.contains("rebalancing");

            if (isGeneration) {
                // 409 Conflict — stale generation, consumer must re-join
                log.warn("Stale generation: {}", msg);
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(Map.of(
                                "error", msg,
                                "hint", "Call POST /api/v1/groups/" + groupId +
                                        "/join with consumerId=" + consumerId + " to rejoin"
                        ));
            } else if (isRebalancing) {
                // 503 — rebalance in progress, retry shortly
                log.info("Rebalance in progress for group {}", groupId);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(Map.of(
                                "error", msg,
                                "hint", "Retry in 1-2 seconds"
                        ));
            } else {
                // Consumer not in group — must join first
                log.warn("Consumer not in group: {}", msg);
                return ResponseEntity.status(HttpStatus.FORBIDDEN)
                        .body(Map.of(
                                "error", msg,
                                "hint", "Call POST /api/v1/groups/" + groupId + "/join first"
                        ));
            }
        }
    }

    @GetMapping("/offset")
    public ResponseEntity<?> getCommittedOffset(
            @RequestParam String topic,
            @RequestParam String group,
            @RequestParam int partition) {

        long committed = consumerService.getCommittedOffset(topic, group, partition);
        long latest    = consumerService.getLatestOffset(topic, partition);
        long lag       = Math.max(0, latest - committed);

        return ResponseEntity.ok(Map.of(
                "topic",           topic,
                "group",           group,
                "partition",       partition,
                "committedOffset", committed,
                "latestOffset",    latest,
                "lag",             lag
        ));
    }
}
