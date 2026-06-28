package com.mq.controller;

import com.mq.cluster.BrokerRegistry;
import com.mq.consumerGroup.ConsumerGroupCoordinator;
import com.mq.model.BrokerInfo;
import com.mq.raft.RaftNode;
import com.mq.storage.LogManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Map;


@Slf4j
@RestController
@RequestMapping("/api/v1/admin")
@RequiredArgsConstructor
public class ShutdownController {

    private final RaftNode raftNode;
    private final BrokerRegistry brokerRegistry;
    private final ConsumerGroupCoordinator coordinator;
    private final LogManager logManager;
    private final RestTemplate restTemplate;
    private final ApplicationContext applicationContext;

    @Value("${broker.id:broker-1}")
    private String brokerId;

    @Value("${broker.host:localhost}")
    private String brokerHost;

    @Value("${server.port:8082}")
    private int brokerPort;


    @PostMapping("/shutdown")
    public ResponseEntity<?> shutdown() {
        log.info("=== Graceful shutdown initiated for {} ===", brokerId);

        try {
            raftNode.stepDown();
            log.info("Shutdown step 1/3: Raft stepped down");
        } catch (Exception e) {
            log.warn("Shutdown step 1: Raft step-down failed (non-fatal): {}", e.getMessage());
        }

        BrokerInfo self = new BrokerInfo(brokerId, brokerHost, brokerPort);
        brokerRegistry.getAllBrokers().stream()
                .filter(b -> !b.getBrokerId().equals(brokerId))
                .forEach(peer -> {
                    try {
                        restTemplate.delete(
                                peer.baseUrl() + "/api/v1/cluster/brokers/" + brokerId);
                        log.info("Shutdown step 2/3: deregistered from {}", peer.getBrokerId());
                    } catch (Exception e) {
                        log.debug("Could not deregister from {}: {}", peer.getBrokerId(), e.getMessage());
                    }
                });

        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(200); // give response time to flush
                log.info("Shutdown step 3/3: closing Spring context");
                ((ConfigurableApplicationContext) applicationContext).close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        shutdownThread.setDaemon(false);
        shutdownThread.setName("graceful-shutdown");
        shutdownThread.start();

        return ResponseEntity.ok(Map.of(
                "message", "Graceful shutdown initiated for " + brokerId,
                "steps", "1) Raft step-down 2) Peer deregister 3) Context close",
                "brokerId", brokerId,
                "note", "Server will stop in ~200ms"
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<?> health() {
        return ResponseEntity.ok(Map.of(
                "brokerId", brokerId,
                "status", "UP",
                "raftState", raftNode.getState().toString(),
                "isLeader", raftNode.isLeader(),
                "term", raftNode.getCurrentTerm()
        ));
    }
}