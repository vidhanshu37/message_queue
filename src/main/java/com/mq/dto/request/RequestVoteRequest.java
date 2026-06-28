package com.mq.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestVoteRequest {
    private int term;

    // Who asking for vote
    private String candidateId;

    // Followers only vote for candidate whose log is at least up-to-date
    private long lastLogOffset;

    // which partition this election is for
    private String topicName;
    private int partitionIdx;
}
