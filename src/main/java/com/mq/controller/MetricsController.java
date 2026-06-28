package com.mq.controller;

import com.mq.service.MetricsService;
import com.mq.storage.RetentionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final MetricsService metricsService;
    private final RetentionManager retentionManager;

    //Full cluster summary
    @GetMapping("/cluster")
    public ResponseEntity<?> getClusterSummary() {
        return ResponseEntity.ok(metricsService.getClusterSummary());
    }

    //Storage stats for one topic
    @GetMapping("/topics/{topicName}")
    public ResponseEntity<?> getTopicStats(@PathVariable String topicName) {
        Map<String, Object> stats = metricsService.getTopicStorageStats(topicName);
        if (stats.containsKey("error")) {
            return ResponseEntity.status(404).body(stats);
        }
        return ResponseEntity.ok(stats);
    }

    //Consumer lag for one group on one topic
    @GetMapping("/lag/{groupId}/{topicName}")
    public ResponseEntity<?> getGroupLag(
            @PathVariable String groupId,
            @PathVariable String topicName) {
        Map<String, Object> lag = metricsService.getGroupLag(groupId, topicName);
        if (lag.containsKey("error")) {
            return ResponseEntity.status(404).body(lag);
        }
        return ResponseEntity.ok(lag);
    }

    //Lag for ALL consumer groups
    @GetMapping("/lag/{topicName}")
    public ResponseEntity<?> getAllGroupsLag(@PathVariable String topicName) {
        return ResponseEntity.ok(metricsService.getAllGroupsLag(topicName));
    }

    // retention policy - by size, time
    @GetMapping("/retention")
    public ResponseEntity<?> getRetentionPolicy() {
        return ResponseEntity.ok(Map.of(
                "retentionHours", retentionManager.getRetentionHours(),
                "retentionMaxBytes", retentionManager.getRetentionMaxBytes(),
                "retentionMaxMB", retentionManager.getRetentionMaxBytes() / (1024 * 1024),
                "checkIntervalMs", retentionManager.getCheckIntervalMs(),
                "checkIntervalMin", retentionManager.getCheckIntervalMs() / 60_000,
                "description", Map.of(
                        "timePolicy", "Delete segments older than " + retentionManager.getRetentionHours() + " hours",
                        "sizePolicy", "Delete oldest segments when partition exceeds " +
                                retentionManager.getRetentionMaxBytes() / (1024 * 1024) + " MB"
                )
        ));
    }

    @PostMapping("/retention/run")
    public ResponseEntity<?> triggerRetention() {
        log.info("Manual retention check triggered via API");
        retentionManager.runNow();
        return ResponseEntity.ok(Map.of(
                "message", "Retention check scheduled — check logs for results",
                "note", "Runs asynchronously on the retention-manager thread"
        ));
    }
}