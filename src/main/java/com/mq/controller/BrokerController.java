package com.mq.controller;

import com.mq.cluster.BrokerRegistry;
import com.mq.cluster.PartitionMetadata;
import com.mq.model.BrokerInfo;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/cluster")
public class BrokerController {
    private final BrokerRegistry brokerRegistry;
    private final PartitionMetadata partitionMetadata;

    /**
     * Register broker with cluster
     */
    @PostMapping("/brokers")
    public ResponseEntity<Map<String, Object>> registerBroker(@Valid @RequestBody BrokerInfo broker) {
        brokerRegistry.registerBroker(broker);

        return ResponseEntity.ok(Map.of(
                "message", "Broker registered successfully",
                "brokerId", broker.getBrokerId(),
                "totalBrokers", brokerRegistry.getBrokerCount(),
                "ringDistribution", brokerRegistry.getAllBrokers()
                        .stream()
                        .map(BrokerInfo::getBrokerId)
                        .toList()
        ));
    }

    /**
     * Deregister broker - called on graceful shutdown
     */
    @DeleteMapping("/brokers/{brokerId}")
    public ResponseEntity<Map<String, Object>> deregisterBroker(@PathVariable String brokerId) {
        brokerRegistry.deregisterBroker(brokerId);

        return ResponseEntity.ok(Map.of(
                "message", "Broker deregister",
                "brokerId", brokerId,
                "remainBrokers", brokerRegistry.getBrokerCount()
        ));
    }


    /**
     * Get the full routing table
     * shows which broker owns which partition
     */
    @GetMapping("/assignments")
    public ResponseEntity<Map<String, Object>> getAssignments() {
        Map<String, Object> response = new HashMap<>();

        partitionMetadata.getAllAssignments().forEach((partition, broker) -> {
            response.put(partition, Map.of(
                    "brokerId", broker.getBrokerId(),
                    "host", broker.getHost(),
                    "port", broker.getPort()
            ));
        });

        return ResponseEntity.ok(response);
    }

    /**
     * Get all registered brokers
     */
    @GetMapping("/brokers")
    public ResponseEntity<?> getAllBrokers() {
        return ResponseEntity.ok(Map.of(
                "brokers", brokerRegistry.getAllBrokers(),
                "count", brokerRegistry.getBrokerCount()
        ));
    }
}
