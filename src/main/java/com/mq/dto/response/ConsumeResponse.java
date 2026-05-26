package com.mq.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class ConsumeResponse {
    private String topicName;
    private int partition;
    private List<MessageDTO> messages;

    // last message offset + 1, so consumer can use for next time
    // if no messages, then nextOffset = current offset
    private long nextOffset;

    // True, if messages were present
    private boolean isEmpty;
}
