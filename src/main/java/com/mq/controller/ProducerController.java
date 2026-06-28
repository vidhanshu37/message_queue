package com.mq.controller;

import com.mq.cluster.BrokerRegistry;
import com.mq.dto.request.ProduceRequest;
import com.mq.dto.response.ProduceResponse;
import com.mq.raft.RaftNode;
import com.mq.service.ProducerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/producer")
@RequiredArgsConstructor
public class ProducerController {
    private final ProducerService producerService;
    private final RaftNode raftNode;
    private final BrokerRegistry brokerRegistry;

    // return 201 - created with location of the produced message (topic, partition, offset)
    @PostMapping("/produce")
    public ResponseEntity<?> produce(@Valid @RequestBody ProduceRequest request) {
        if (!raftNode.isLeader()) {
            String leaderId = raftNode.getCurrentLeaderId();

            if (leaderId == null) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(Map.of("error", "No leader elected yet"));
            }

            // Find leader's URL from broker registry
            return brokerRegistry.getAllBrokers().stream()
                    .filter(b -> b.getBrokerId().equals(leaderId))
                    .findFirst()
                    .map(leader -> {
                        String redirectUrl = leader.baseUrl()
                                + "/api/v1/producer/produce";
                        log.info("Not leader, redirecting to: {}", redirectUrl);

                        org.springframework.http.HttpHeaders headers =
                                new org.springframework.http.HttpHeaders();
                        headers.add("Location", redirectUrl);

                        return ResponseEntity
                                .status(HttpStatus.TEMPORARY_REDIRECT)
                                .headers(headers)
                                .body(null);
                    })
                    .orElse(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "Leader " + leaderId
                                    + " not found in registry")));
        }

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(producerService.produce(request));
    }
}
