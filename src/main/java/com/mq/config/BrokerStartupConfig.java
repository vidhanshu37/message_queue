package com.mq.config;

import com.mq.cluster.BrokerRegistry;
import com.mq.model.BrokerInfo;
import com.mq.repository.TopicRepository;
import com.mq.storage.LogManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BrokerStartupConfig {

    private final BrokerRegistry  brokerRegistry;
    private final RestTemplate    restTemplate;
    private final TopicRepository topicRepository;
    private final LogManager      logManager;

    @Value("${broker.id:broker-1}")
    private String brokerId;

    @Value("${broker.host:localhost}")
    private String brokerHost;

    @Value("${server.port:8082}")
    private int brokerPort;

    @Value("${cluster.peers:localhost:8082,localhost:8083,localhost:8084}")
    private String clusterPeers;

    @Bean
    @Order(1)
    public ApplicationRunner selfRegister() {
        return args -> {
            BrokerInfo self = new BrokerInfo(brokerId, brokerHost, brokerPort);
            brokerRegistry.registerBroker(self);
            log.info("Self registered: {} in local registry", self);

            List<String> peerUrls = parsePeerUrls(self);

            for (String peerUrl : peerUrls) {
                try {
                    BrokerInfo[] knownBrokers = restTemplate.postForObject(
                            peerUrl + "/api/v1/cluster/brokers/join",
                            self,
                            BrokerInfo[].class
                    );

                    if (knownBrokers != null) {
                        for (BrokerInfo known : knownBrokers) {
                            if (!known.getBrokerId().equals(brokerId)) {
                                brokerRegistry.registerBroker(known);
                                log.info("Learned about peer {} from {}", known.getBrokerId(), peerUrl);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Peer at {} not reachable yet: {}", peerUrl, e.getMessage());
                }
            }

            log.info("Cluster bootstrap complete. Known brokers: {}",
                    brokerRegistry.getAllBrokers().stream()
                            .map(BrokerInfo::getBrokerId)
                            .toList());
        };
    }

    @Bean
    @Order(2)
    public ApplicationRunner restorePartitionLogs() {
        return args -> {
            log.info("Restoring partition logs from DB after startup...");
            int restored = 0;

            for (var topic : topicRepository.findAll()) {
                for (int p = 0; p < topic.getPartitionCount(); p++) {
                    try {
                        logManager.initPartition(topic.getName(), p);
                        restored++;
                    } catch (Exception e) {
                        log.warn("Failed to restore partition {}-{}: {}",
                                topic.getName(), p, e.getMessage());
                    }
                }
            }

            log.info("Partition log restore complete. {} partitions initialized.", restored);
        };
    }

    private List<String> parsePeerUrls(BrokerInfo self) {
        return java.util.Arrays.stream(clusterPeers.split(","))
                .map(String::trim)
                .filter(addr -> {
                    String[] parts = addr.split(":");
                    if (parts.length != 2) return false;
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    return !(host.equals(self.getHost()) && port == self.getPort());
                })
                .map(addr -> "http://" + addr)
                .toList();
    }
}