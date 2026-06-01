package com.mq.config;

import com.mq.cluster.BrokerRegistry;
import com.mq.model.BrokerInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BrokerStartupConfig {
    private final BrokerRegistry brokerRegistry;

    @Value("${broker.id:broker-1}")
    private String brokerId;

    @Value("${broker.host:localhost}")
    private String brokerHost;

    @Value("${broker.port:8082}")
    private int brokerPort;

    // On application startup, register this broker in the cluster registry
    @Bean
    public ApplicationRunner selfRegister() {
        return args -> {
            BrokerInfo self = new BrokerInfo(brokerId, brokerHost, brokerPort);
            brokerRegistry.registerBroker(self);
            log.info("self registered Broker {} registered in cluster registry", self);
        };
    }
}
