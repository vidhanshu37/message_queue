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
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
public class TopicService {

    private final TopicRepository topicRepository;
    private final PartitionRepository partitionRepository;
    private final LogManager logManager;
    private final BrokerRegistry brokerRegistry;

    @Transactional
    public TopicResponse createTopic(CreateTopicRequest request) {
        if(topicRepository.existsByName(request.getName())) {
            throw new TopicAlreadyExistsException(request.getName());
        }

        Topic topic = new Topic(request.getName(), request.getPartitionCount());
        topic = topicRepository.save(topic);

        // Create partitions for the topic
        Topic finalTopic = topic;
        List<Partition> partitions = IntStream.range(0, request.getPartitionCount())
                .mapToObj(i -> new Partition(finalTopic, i))
                .toList();
        partitionRepository.saveAll(partitions);

        for(int i=0; i<request.getPartitionCount(); i++) {
            try {
                logManager.initPartition(finalTopic.getName(), i);

            } catch (IOException e) {
                throw new RuntimeException("Failed to create log for partition " + i, e);
            }
        }
        if(brokerRegistry.getBrokerCount() > 0) {
            brokerRegistry.assignPartitions(
                    request.getName(),
                    request.getPartitionCount()
            );
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
