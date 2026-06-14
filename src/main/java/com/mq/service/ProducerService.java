package com.mq.service;

import com.mq.dto.request.ProduceRequest;
import com.mq.dto.response.ProduceResponse;
import com.mq.exception.TopicNotFoundException;
import com.mq.model.Topic;
import com.mq.replication.ReplicationService;
import com.mq.repository.TopicRepository;
import com.mq.storage.LogManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerService {

    private final TopicRepository topicRepository;
    private final LogManager logManager;
    private final PartitionSelector partitionSelector;
    private final ReplicationService replicationService;

    /**
     * Core operation by producer service
     * 1. validate topic exists
     * 2. select target partition (round-robin or key-based)
     * 3. write to log
     * 4. return location (partition + offset)
     */

    public ProduceResponse produce(ProduceRequest request) {
        // step1: does this topic is existing (use db)
        Topic topic = topicRepository.findByName(request.getTopicName())
                .orElseThrow(() -> new TopicNotFoundException(request.getTopicName()));

        // step2: select partition for this message
        int partitionIdx = partitionSelector.selectPartition(
                request.getTopicName(),
                request.getKey(),
                topic.getPartitionCount()
        );

        // step3: convert string value to byte
        // we store raw bytes -- the producer can choose any serialization format (e.g. JSON, Avro, Protobuf)
        byte[] payload = request.getValue().getBytes();

        try {
            // step1: write to leader's local log
            long offset = logManager.append(
                    request.getTopicName(),
                    partitionIdx,
                    payload
            );

            // step2: replicate to followers (async)
            // only confirm to producer after majority acks
            boolean replicated = replicationService.replicationToFollowers(
                    request.getTopicName(),
                    partitionIdx,
                    payload,
                    offset
            );

            if(!replicated) {
                // majority replicated fail
                log.warn("Failed to replicate message to majority of followers for {}-{} at offset {}",
                        request.getTopicName(), partitionIdx, offset);
            }

            log.info("produced message to {}-{} at offset {}", request.getTopicName(),partitionIdx, offset);

            return new ProduceResponse(
                    request.getTopicName(),
                    partitionIdx,
                    offset,
                    replicated ? "Message produced and replicated" : "Message produced (replication degraded)"
            );
        } catch (IOException e) {
            log.error("Failed to produce message to {}-{}: {}", request.getTopicName(), partitionIdx, e.getMessage());
            throw new RuntimeException("Storage failure: " + e.getMessage(), e);
        }

    }


}
