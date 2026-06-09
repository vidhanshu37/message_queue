package com.mq.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReplicateRequest {

    // which partition this message belongs to
    private String topicName;
    private int partitionIdx;

    // The message itself
    // Base64 encoded because HTTP JSON body is text, and message can be binary
    private String payloadBase64;

    // The offset of the message in the partition
    // Both leader and follower must agree on offset
    private long offset;
}
