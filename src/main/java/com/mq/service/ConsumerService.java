package com.mq.service;

import com.mq.consumerGroup.ConsumerGroupCoordinator;
import com.mq.consumerGroup.GroupState;
import com.mq.dto.response.AssignmentResponse;
import com.mq.dto.response.ConsumeResponse;
import com.mq.dto.response.MessageDTO;
import com.mq.exception.TopicNotFoundException;
import com.mq.model.ConsumerOffset;
import com.mq.repository.ConsumerOffsetRepository;
import com.mq.repository.TopicRepository;
import com.mq.storage.LogManager;
import com.mq.storage.MessageRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerService {

    private static final int MAX_BYTES_FETCH = 1024 * 1024; // 1 MB cap

    private final TopicRepository            topicRepository;
    private final ConsumerOffsetRepository   consumerOffsetRepository;
    private final LogManager                 logManager;
    private final ConsumerGroupCoordinator coordinator;


    @Transactional
    public ConsumeResponse consume(String topicName, String groupId,
                                   int partitionIdx, long requestedOffset) {

        topicRepository.findByName(topicName)
                .orElseThrow(() -> new TopicNotFoundException(topicName));

        long startOffset = resolveStartOffset(topicName, groupId, partitionIdx, requestedOffset);

        List<MessageRecord> records = readFromLog(topicName, partitionIdx, startOffset);

        if (records.isEmpty()) {
            log.debug("No messages for {}-{} at offset {}", topicName, partitionIdx, startOffset);
            return new ConsumeResponse(topicName, partitionIdx, List.of(), startOffset, true);
        }

        long lastOffset = records.get(records.size() - 1).getOffset();
        commitOffset(topicName, groupId, partitionIdx, lastOffset + 1);

        List<MessageDTO> messages = records.stream()
                .map(r -> new MessageDTO(r.getOffset(), new String(r.getPayload())))
                .toList();

        log.info("Consumed {} messages from {}-{} startOffset={} nextOffset={}",
                messages.size(), topicName, partitionIdx, startOffset, lastOffset + 1);

        return new ConsumeResponse(topicName, partitionIdx, messages, lastOffset + 1, false);
    }

    @Transactional
    public ConsumeResponse consumeAsGroupMember(String topicName,
                                                String groupId,
                                                String consumerId,
                                                int partitionIdx,
                                                int generation,
                                                long requestedOffset) {

        topicRepository.findByName(topicName)
                .orElseThrow(() -> new TopicNotFoundException(topicName));

        com.mq.dto.response.AssignmentResponse assignment = coordinator.getAssignment(groupId, consumerId);

        if (assignment.getGeneration() == -1) {
            throw new IllegalStateException(
                    "Consumer '" + consumerId + "' is not a member of group '" + groupId +
                            "'. Call POST /api/v1/groups/" + groupId + "/join first.");
        }

        if (assignment.getGroupState() == GroupState.PREPARING_REBALANCE) {
            throw new IllegalStateException(
                    "Group '" + groupId + "' is rebalancing (generation=" + assignment.getGeneration() +
                            "). Wait for STABLE state then re-fetch your assignment.");
        }

        if (generation != assignment.getGeneration()) {
            throw new IllegalStateException(
                    "Stale generation: consumer has generation=" + generation +
                            " but coordinator is at generation=" + assignment.getGeneration() +
                            ". Call POST /api/v1/groups/" + groupId + "/join to rejoin.");
        }

        List<Integer> ownedPartitions = assignment.getAssignedPartitions();
        if (!ownedPartitions.contains(partitionIdx)) {
            throw new IllegalArgumentException(
                    "Consumer '" + consumerId + "' does not own partition " + partitionIdx +
                            " in group '" + groupId + "'. Owned partitions: " + ownedPartitions +
                            ". This partition may have been reassigned during rebalance.");
        }

        long startOffset = resolveStartOffset(topicName, groupId, partitionIdx, requestedOffset);

        List<MessageRecord> records = readFromLog(topicName, partitionIdx, startOffset);

        if (records.isEmpty()) {
            log.debug("No new messages for group={} consumer={} partition={} offset={}",
                    groupId, consumerId, partitionIdx, startOffset);
            return new ConsumeResponse(topicName, partitionIdx, List.of(), startOffset, true);
        }

        long lastOffset = records.get(records.size() - 1).getOffset();
        commitOffset(topicName, groupId, partitionIdx, lastOffset + 1);

        List<MessageDTO> messages = records.stream()
                .map(r -> new MessageDTO(r.getOffset(), new String(r.getPayload())))
                .toList();

        log.info("Group consume: group={} consumer={} partition={} messages={} nextOffset={}",
                groupId, consumerId, partitionIdx, messages.size(), lastOffset + 1);

        return new ConsumeResponse(topicName, partitionIdx, messages, lastOffset + 1, false);
    }

    private long resolveStartOffset(String topicName, String groupId,
                                    int partitionIdx, long requestedOffset) {
        if (requestedOffset >= 0) {
            return requestedOffset;
        }
        return consumerOffsetRepository
                .findByGroupIdAndTopicNameAndPartitionIndex(groupId, topicName, partitionIdx)
                .map(ConsumerOffset::getCommittedOffset)
                .orElse(0L);
    }

    private List<MessageRecord> readFromLog(String topicName, int partitionIdx, long startOffset) {
        try {
            return logManager.read(topicName, partitionIdx, startOffset, MAX_BYTES_FETCH);
        } catch (IOException e) {
            log.error("Failed to read {}-{} at offset {}: {}", topicName, partitionIdx, startOffset, e.getMessage());
            throw new RuntimeException("Storage read failure: " + e.getMessage(), e);
        }
    }

    @Transactional
    public void commitOffset(String topicName, String groupId,
                             int partitionIdx, long nextOffset) {
        ConsumerOffset co = consumerOffsetRepository
                .findByGroupIdAndTopicNameAndPartitionIndex(groupId, topicName, partitionIdx)
                .orElse(new ConsumerOffset(groupId, topicName, partitionIdx));
        co.setCommittedOffset(nextOffset);
        consumerOffsetRepository.save(co);
    }

    @Transactional(readOnly = true)
    public long getLatestOffset(String topicName, int partitionIdx) {
        try {
            return logManager.getLatestOffset(topicName, partitionIdx);
        } catch (IOException e) {
            throw new RuntimeException("Cannot get latest offset", e);
        }
    }

    @Transactional(readOnly = true)
    public long getCommittedOffset(String topicName, String groupId, int partitionIdx) {
        return consumerOffsetRepository
                .findByGroupIdAndTopicNameAndPartitionIndex(groupId, topicName, partitionIdx)
                .map(ConsumerOffset::getCommittedOffset)
                .orElse(0L);
    }
}