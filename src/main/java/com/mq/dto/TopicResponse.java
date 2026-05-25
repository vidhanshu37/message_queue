package com.mq.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class TopicResponse {
    private Long id;
    private String name;
    private int partitionCount;
    private Instant createdAt;
}
