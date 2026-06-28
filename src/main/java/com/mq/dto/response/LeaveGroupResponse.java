package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LeaveGroupResponse {
    private String groupId;
    private String consumerId;
    private boolean success;
    private String message;
}
