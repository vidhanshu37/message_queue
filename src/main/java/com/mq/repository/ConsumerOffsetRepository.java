package com.mq.repository;

import com.mq.model.ConsumerOffset;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ConsumerOffsetRepository extends JpaRepository<ConsumerOffset, Long> {
    Optional<ConsumerOffset> findByGroupIdAndTopicNameAndPartitionIndex(
            String groupId, String topicName, int partitionIndex
    );
}
