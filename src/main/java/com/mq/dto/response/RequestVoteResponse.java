package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestVoteResponse {

    // voter's current term
    private int term;

    // did the voter grant vote to candidate?
    private boolean voteGranted;

    private String voterId;
}
