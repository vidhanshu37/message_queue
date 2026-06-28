package com.mq.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JoinGroupRequest {
    private String groupId;
    private String consumerId;
    private String topicName;
    private long sessionTimeoutMs = 30_000;
}
