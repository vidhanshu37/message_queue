package com.mq.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "consumer_offsets",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"groupId", "topic_name", "partition_index"}
        ))
@Getter
@Setter
@NoArgsConstructor
public class ConsumerOffset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String groupId;

    @Column(nullable = false, name = "topic_name")
    private String topicName;

    @Column(nullable = false, name = "partition_index")
    private int partitionIndex;

    @Column(nullable = false)
    private long committedOffset = 0L;

    public ConsumerOffset(String groupId, String topicName, int partitionIndex) {
        this.groupId = groupId;
        this.topicName = topicName;
        this.partitionIndex = partitionIndex;
    }
}
