package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ProduceResponse {
    private String topicName;
    private int partition;
    private long offset;

    // Human-readable confirmation
    private String message;

}
