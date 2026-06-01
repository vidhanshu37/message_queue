package com.mq.service;

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

    // it prevents huge response, max 1MB
    private static final int MAX_BYTES_FETCH = 1024 * 1024;

    private final TopicRepository topicRepository;
    private final ConsumerOffsetRepository consumerOffsetRepository;
    private final LogManager logManager;

    /**
     * 1. Validate topic exists
     * 2. determine start offset
     *      - if offset is provided, use it
     *      - else, use the last committed offset for this consumer group
     *      - else, start from offset 0
     * 3. Read msg from log
     * 4. Auto-commit the new offset
     * 5. return msg and nextOffset
     */

    @Transactional
    public ConsumeResponse consume(String topicName, String groupId, int partitionIdx, long requestedOffset) {
        // step 1 - validate topic exists or not
        topicRepository.findByName(topicName)
                .orElseThrow(() -> new TopicNotFoundException(topicName));

        // step 2 - determine start offset
        long startOffset = resolveStartOffset(topicName, groupId, partitionIdx, requestedOffset);

        // step 3 - read messages from log
        List<MessageRecord> records;
        try {
            records = logManager.read(topicName, partitionIdx, startOffset, MAX_BYTES_FETCH);
        } catch (IOException e) {
            log.error("Failed to read from log: {}", e.getMessage());
            throw new RuntimeException("Storage read failure : " + e.getMessage(), e);
        }

        // handle empty partition
        if(records.isEmpty()) {
            log.debug("no messages found for {}-{} at offset {}", topicName, partitionIdx, startOffset);
            return new ConsumeResponse(topicName, partitionIdx, List.of(), startOffset, true);
        }

        // step 4 - Auto commit offset
        long lastOffset = records.get(records.size() - 1).getOffset();
        commitOffset(topicName, groupId, partitionIdx, lastOffset + 1);

        // step 5 - return response
        List<MessageDTO> messages = records.stream()
                .map(r -> new MessageDTO(r.getOffset(), new String(r.getPayload())))
                .toList();

        log.info("Consumed {} messages from {}-{} at offset {}, nextOffset {}",
                messages.size(), topicName, partitionIdx, startOffset, lastOffset + 1);

        return new ConsumeResponse(topicName, partitionIdx, messages, lastOffset + 1, false);
    }
    /**
     * Determine start offset
     *
     * case 1 - if requestedOffset >= 0, then use it
     * case 2 - if requestedOffset == -1, then look in db for last commited offset
     * case 3 - if no commited offset exists, then start from 0
     */
    private long resolveStartOffset(String topicName, String groupId, int partitionIdx, long requestedOffset) {
        if(requestedOffset >= 0) {
            return requestedOffset;
        }

        // last committed offset for this group+partition
        return consumerOffsetRepository.findByGroupIdAndTopicNameAndPartitionIndex(
                groupId, topicName, partitionIdx)
                .map(ConsumerOffset::getCommittedOffset) // start from next offset
                .orElse(0L);
    }

    /**
     *  Save or update(upsert) committed offset
     */
    @Transactional
    public void commitOffset(String topicName, String groupId, int partitionIdx, long nextOffset) {
        ConsumerOffset consumerOffset = consumerOffsetRepository
                .findByGroupIdAndTopicNameAndPartitionIndex(
                        groupId, topicName, partitionIdx)
                .orElse(new ConsumerOffset(groupId, topicName, partitionIdx));

        consumerOffset.setCommittedOffset(nextOffset);
        consumerOffsetRepository.save(consumerOffset);
    }

    /**
     * get latest available offset for partition
     * lag = lastestOffset - committedOffset
     */
    @Transactional(readOnly = true)
    public long getLatestOffset(String topicName, int partitionIdx) {
        try {
            return logManager.getLatestOffset(topicName, partitionIdx);
        } catch (IOException e) {
            throw new RuntimeException("Cannot get latest offset", e);
        }
    }

    /**
     * Get committed offset for specific group + partition
     */
    @Transactional(readOnly = true)
    public long getCommittedOffset(String topicName, String groupId, int partitionIdx) {
        return consumerOffsetRepository
                .findByGroupIdAndTopicNameAndPartitionIndex(groupId, topicName, partitionIdx)
                .map(ConsumerOffset::getCommittedOffset)
                .orElse(0L);
    }

}
