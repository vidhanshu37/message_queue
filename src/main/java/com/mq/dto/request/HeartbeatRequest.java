package com.mq.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HeartbeatRequest {
    // leader's current term
    private int term;

    // leader's id
    private String leaderId;

    // which partition this heartbeat is for
    private String topicName;
    private int partitionIdx;

    // Leader's latest offset — followers use this
    // to know if they need to catch up
    private long leaderOffset;
}
