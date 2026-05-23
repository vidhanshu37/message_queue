package com.mq.repository;

import com.mq.model.Partition;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface PartitionRepository extends JpaRepository<Partition, Long> {
    List<Partition> findByTopicNameOrderByPartitionIndex(String topicName);
    Optional<Partition> findByTopicNameAndPartitionIndex(String topicName, int partitionIndex);
}