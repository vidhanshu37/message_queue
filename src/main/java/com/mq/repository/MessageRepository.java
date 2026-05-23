//package com.mq.repository;
//import com.mq.model.Message;
//import org.springframework.data.jpa.repository.JpaRepository;
//import java.util.List;
//
//public interface MessageRepository extends JpaRepository<Message, Long> {
//
//    // This is the core query — "give me N messages from offset Z"
//    List<Message> findByTopicNameAndPartitionIndexAndOffsetGreaterThanEqualOrderByOffsetAsc(
//            String topicName, int partitionIndex, long offset
//    );
//}