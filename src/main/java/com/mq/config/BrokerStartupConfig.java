package com.mq.config;

import com.mq.cluster.BrokerRegistry;
import com.mq.model.BrokerInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BrokerStartupConfig {

    private final BrokerRegistry brokerRegistry;
    private final RestTemplate restTemplate;

    @Value("${broker.id:broker-1}")
    private String brokerId;

    @Value("${broker.host:localhost}")
    private String brokerHost;

    @Value("${server.port:8082}")
    private int brokerPort;


    @Value("${cluster.peers:localhost:8082,localhost:8083,localhost:8084}")
    private String clusterPeers;

    @Bean
    public ApplicationRunner selfRegister() {
        return args -> {
            BrokerInfo self = new BrokerInfo(brokerId, brokerHost, brokerPort);

            // Step 1: Register self locally
            brokerRegistry.registerBroker(self);
            log.info("Self registered: {} in local registry", self);

            // Step 2: Parse peer addresses from config
            List<String> peerUrls = parsePeerUrls(self);

            // Step 3: For each peer, announce ourselves AND learn about them
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
                    // Peer is not up yet — that's fine, it will contact us when it starts
                    log.debug("Peer at {} not reachable yet: {}", peerUrl, e.getMessage());
                }
            }

            log.info("Cluster bootstrap complete. Known brokers: {}",
                    brokerRegistry.getAllBrokers().stream()
                            .map(BrokerInfo::getBrokerId)
                            .toList());
        };
    }

    private List<String> parsePeerUrls(BrokerInfo self) {
        return java.util.Arrays.stream(clusterPeers.split(","))
                .map(String::trim)
                .filter(addr -> {
                    // Exclude ourselves
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