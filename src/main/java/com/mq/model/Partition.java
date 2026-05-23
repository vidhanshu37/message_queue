package com.mq.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "partitions",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"topic_id", "partitionIndex"}
        ))
@Getter
@Setter
@NoArgsConstructor
public class Partition {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "topic_id", nullable = false)
    private Topic topic;

    @Column(nullable = false)
    private int partitionIndex; // 0-based index within the topic

    @Column(nullable = false)
    private long nextOffSet = 0L;

    public Partition(Topic topic, int partitionIndex) {
        this.topic = topic;
        this.partitionIndex = partitionIndex;
    }
}
