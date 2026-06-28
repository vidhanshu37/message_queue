package com.mq.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HeartbeatGroupRequest {
    private String groupId;
    private String consumerId;

    private int generation;
}
