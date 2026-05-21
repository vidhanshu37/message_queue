package com.mq.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "consumer_offsets",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"groupId", "topicName", "partitionIndex"}
        ))
@Data
@NoArgsConstructor
public class ConsumerOffset {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;

        @Column(nullable = false)
        private String groupId;

        @Column(nullable = false)
        private String topicName;

        @Column(nullable = false)
        private int partitionIndex;

        @Column(nullable = false)
        private long offset = 0L;
}
