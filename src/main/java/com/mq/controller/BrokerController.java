package com.mq.controller;

import com.mq.cluster.BrokerRegistry;
import com.mq.cluster.PartitionMetadata;
import com.mq.model.BrokerInfo;
import com.mq.replication.ReplicationService;
import com.mq.storage.LogManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/cluster")
public class BrokerController {

    private final BrokerRegistry brokerRegistry;
    private final PartitionMetadata partitionMetadata;
    private final LogManager logManager;
    private final ReplicationService replicationService;

    @PostMapping("/brokers/join")
    public ResponseEntity<Collection<BrokerInfo>> joinCluster(@RequestBody BrokerInfo newBroker) {
        log.info("Broker {} joining cluster", newBroker.getBrokerId());

        // Register the newcomer in registry
        brokerRegistry.registerBroker(newBroker);

        // Return everything we know so the caller can populate its own registry
        Collection<BrokerInfo> allKnown = brokerRegistry.getAllBrokers();
        log.info("Returning {} known brokers to {}", allKnown.size(), newBroker.getBrokerId());

        return ResponseEntity.ok(allKnown);
    }


    @PostMapping("/brokers")
    public ResponseEntity<Map<String, Object>> registerBroker(@RequestBody BrokerInfo broker) {
        brokerRegistry.registerBroker(broker);
        return ResponseEntity.ok(Map.of(
                "message", "Broker registered successfully",
                "brokerId", broker.getBrokerId(),
                "totalBrokers", brokerRegistry.getBrokerCount(),
                "ringDistribution", brokerRegistry.getAllBrokers()
                        .stream().map(BrokerInfo::getBrokerId).toList()
        ));
    }

    /**
     * Deregister broker — called on graceful shutdown.
     */
    @DeleteMapping("/brokers/{brokerId}")
    public ResponseEntity<Map<String, Object>> deregisterBroker(@PathVariable String brokerId) {
        brokerRegistry.deregisterBroker(brokerId);
        return ResponseEntity.ok(Map.of(
                "message", "Broker deregistered",
                "brokerId", brokerId,
                "remainBrokers", brokerRegistry.getBrokerCount()
        ));
    }

    /**
     * Get full routing table — which broker owns which partition.
     */
    @GetMapping("/assignments")
    public ResponseEntity<Map<String, Object>> getAssignments() {
        Map<String, Object> response = new HashMap<>();
        partitionMetadata.getAllAssignments().forEach((partition, broker) ->
                response.put(partition, Map.of(
                        "brokerId", broker.getBrokerId(),
                        "host", broker.getHost(),
                        "port", broker.getPort()
                )));
        return ResponseEntity.ok(response);
    }

    /**
     * Get all registered brokers.
     */
    @GetMapping("/brokers")
    public ResponseEntity<?> getAllBrokers() {
        return ResponseEntity.ok(Map.of(
                "brokers", brokerRegistry.getAllBrokers(),
                "count", brokerRegistry.getBrokerCount()
        ));
    }

    /**
     * Get replication status for a partition.
     */
    @GetMapping("/replication/{topic}/{partition}")
    public ResponseEntity<?> getReplicationStatus(
            @PathVariable String topic, @PathVariable int partition) {
        try {
            long leaderOffset = logManager.getLatestOffset(topic, partition);
            Map<String, Object> metrics =
                    replicationService.getReplicationMetrics(topic, partition, leaderOffset);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }
}