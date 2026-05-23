//package com.mq.model;
//
//import jakarta.persistence.*;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.hibernate.annotations.CreationTimestamp;
//
//import java.time.LocalDateTime;
//
//@Entity
//@Table(name = "messages", indexes = {
//        @Index(name = "idx_topic_partition_offset", columnList = "topicName, partitionIndex, offset")
//})
//@Data
//@NoArgsConstructor
//public class Message {
//    @Id
//    @GeneratedValue(strategy = GenerationType.IDENTITY)
//    private Long id;
//
//    @Column(nullable = false)
//    private String topicName;
//
//    @Column(nullable = false)
//    private int partitionIndex;
//
//    @Column(nullable = false)
//    private long offset; // unique within topic-partition, assigned sequentially
//
//    private String messageKey;
//
//    @Lob
//    @Column(nullable = false)
//    private byte[] payload;
//
//    @CreationTimestamp
//    private LocalDateTime timeStamp;
//}
