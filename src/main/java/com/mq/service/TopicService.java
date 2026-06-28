package com.mq.service;

import com.mq.cluster.BrokerRegistry;
import com.mq.dto.request.CreateTopicRequest;
import com.mq.dto.response.TopicResponse;
import com.mq.exception.TopicAlreadyExistsException;
import com.mq.exception.TopicNotFoundException;
import com.mq.model.Partition;
import com.mq.model.Topic;
import com.mq.repository.PartitionRepository;
import com.mq.repository.TopicRepository;
import com.mq.storage.LogManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
@Service
public class TopicService {

    private final TopicRepository topicRepository;
    private final PartitionRepository partitionRepository;
    private final LogManager logManager;
    private final BrokerRegistry brokerRegistry;
    private final RestTemplate restTemplate;
    private final String currentBrokerId;

    public TopicService(TopicRepository topicRepository,
                        PartitionRepository partitionRepository,
                        LogManager logManager,
                        BrokerRegistry brokerRegistry,
                        RestTemplate restTemplate,
                        @Value("${broker.id:broker-1}") String currentBrokerId) {
        this.topicRepository = topicRepository;
        this.partitionRepository = partitionRepository;
        this.logManager = logManager;
        this.brokerRegistry = brokerRegistry;
        this.restTemplate = restTemplate;
        this.currentBrokerId = currentBrokerId;
    }

    @Transactional
    public TopicResponse createTopic(CreateTopicRequest request) {
        if (topicRepository.existsByName(request.getName())) {
            throw new TopicAlreadyExistsException(request.getName());
        }

        Topic topic = new Topic(request.getName(), request.getPartitionCount());
        topic = topicRepository.save(topic);

        Topic finalTopic = topic;
        List<Partition> partitions = IntStream.range(0, request.getPartitionCount())
                .mapToObj(i -> new Partition(finalTopic, i))
                .toList();
        partitionRepository.saveAll(partitions);

        // Initialize each partition locally AND on all followers
        for (int i = 0; i < request.getPartitionCount(); i++) {
            final int partitionIndex = i;

            // Step 1: init on THIS broker
            try {
                logManager.initPartition(request.getName(), partitionIndex);
                log.info("Initialized partition {}-{} locally on {}",
                        request.getName(), partitionIndex, currentBrokerId);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to create log for partition " + partitionIndex, e);
            }

            // Step 2: tell ALL other brokers to init this partition too
            brokerRegistry.getAllBrokers().stream()
                    .filter(b -> !b.getBrokerId().equals(currentBrokerId))
                    .forEach(broker -> {
                        try {
                            String url = broker.baseUrl()
                                    + "/api/v1/replication/init-partition";
                            restTemplate.postForObject(
                                    url,
                                    Map.of(
                                            "topicName", request.getName(),
                                            "partitionIndex", partitionIndex
                                    ),
                                    Void.class
                            );
                            log.info("Initialized partition {}-{} on remote broker {}",
                                    request.getName(), partitionIndex,
                                    broker.getBrokerId());
                        } catch (Exception e) {
                            log.warn("Could not init partition {}-{} on broker {}: {}",
                                    request.getName(), partitionIndex,
                                    broker.getBrokerId(), e.getMessage());
                        }
                    });
        }

        // Assign partitions to brokers via consistent hashing
        if (brokerRegistry.getBrokerCount() > 0) {
            brokerRegistry.assignPartitions(
                    request.getName(),
                    request.getPartitionCount());
        }

        return toResponse(topic);
    }

    @Transactional(readOnly = true)
    public TopicResponse getTopic(String name) {
        Topic topic = topicRepository.findByName(name)
                .orElseThrow(() -> new TopicNotFoundException(name));
        return toResponse(topic);
    }

    @Transactional(readOnly = true)
    public List<TopicResponse> getAllTopics() {
        return topicRepository.findAll().stream()
                .map(this::toResponse)
                .toList();
    }

    private TopicResponse toResponse(Topic topic) {
        return new TopicResponse(
                topic.getId(),
                topic.getName(),
                topic.getPartitionCount(),
                topic.getCreatedAt()
        );
    }
}