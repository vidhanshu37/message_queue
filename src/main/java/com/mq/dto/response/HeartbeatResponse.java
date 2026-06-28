package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HeartbeatResponse {
    private int term;
    private boolean success;
    private String followerId;
}
