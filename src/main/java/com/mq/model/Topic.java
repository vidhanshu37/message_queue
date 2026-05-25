package com.mq.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "topics")
@Getter
@Setter
@NoArgsConstructor
@ToString(exclude = "partitions")  // avoid infinite loop in logs
public class Topic {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private int partitionCount;

    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    // One topic may own many partition - if topic is deleted, partitions is also deleted
    @OneToMany(mappedBy = "topic", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<Partition> partitions = new ArrayList<>();

    public Topic(String name, int partitionCount) {
        this.name = name;
        this.partitionCount = partitionCount;
    }
}
