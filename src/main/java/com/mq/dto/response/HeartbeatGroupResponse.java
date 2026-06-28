package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HeartbeatGroupResponse {
    private String groupId;
    private String consumerId;

    private int generation;

    private boolean success;

    private boolean rebalanceRequired;

    private String message;
}
