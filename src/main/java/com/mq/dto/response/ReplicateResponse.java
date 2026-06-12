package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReplicateResponse {

    private boolean success;
    private String brokerId;
    private long replicatedOffset;
    private String message;
}
