package com.mq.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReplicateRequest {

    private String topicName;
    private int    partitionIdx;

    private String payloadBase64;

    private long offset;

    private String leaderId;
}