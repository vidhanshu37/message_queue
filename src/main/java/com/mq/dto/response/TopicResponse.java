package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
public class TopicResponse {
    private Long id;
    private String name;
    private int partitionCount;
    private Instant createdAt;
}
