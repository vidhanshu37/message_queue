package com.mq.service;

import com.mq.dto.CreateTopicRequest;
import com.mq.dto.TopicResponse;
import com.mq.exception.TopicAlreadyExistsException;
import com.mq.exception.TopicNotFoundException;
import com.mq.model.Partition;
import com.mq.model.Topic;
import com.mq.repository.PartitionRepository;
import com.mq.repository.TopicRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
public class TopicService {

    private final TopicRepository topicRepository;
    private final PartitionRepository partitionRepository;

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
