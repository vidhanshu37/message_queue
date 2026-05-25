package com.mq.storage;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class MessageRecord {
    private final long offset;
    private final byte[] payload;

    public int serializedSize() {
        // 8 bytes for offset + 4 bytes for payload length + payload
        return 8 + 4 + payload.length;
    }
}
