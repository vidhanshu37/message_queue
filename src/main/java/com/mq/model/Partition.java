package com.mq.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "partitions",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"topicName", "partitionIndex"}
        ))
@Data
@NoArgsConstructor
public class Partition {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String topicName;

    @Column(nullable = false)
    private int partitionIndex;

    @Column(nullable = false)
    private long lastestOffset = 0L; // grows as messages arrive
}
