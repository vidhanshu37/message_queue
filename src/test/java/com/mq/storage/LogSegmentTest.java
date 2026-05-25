package com.mq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;


public class LogSegmentTest {
    @TempDir
    Path tempDir;

    private LogSegment createSegment(long baseOffset) throws IOException {
        Path log = tempDir.resolve(baseOffset + ".log");
        Path idx = tempDir.resolve(baseOffset + ".idx");

        return new LogSegment(log, idx, baseOffset);
    }

    @Test
    void appendAndReadBack_singleMessage() throws IOException {
         try(LogSegment segment = createSegment(0)) {
             byte[] payload = "hello-world".getBytes();
             long offset = segment.append(payload);

             List<MessageRecord> records = segment.read(0, 1024);

             assertThat(offset).isEqualTo(0L);
             assertThat(records).hasSize(1);
             assertThat(records.get(0).getPayload()).isEqualTo(payload);
         }
    }

    @Test
    void offsetsAreSequential() throws IOException {
        try(LogSegment segment = createSegment(0)) {
            for(int i=0; i<10; i ++) {
                long offset = segment.append(("message-" + i).getBytes());
                assertThat(offset).isEqualTo(i);
            }
        }
    }

    @Test
    void readFrom_midOffset_returnOnlyLaterMessages() throws IOException {
        try(LogSegment segment = createSegment(0)) {
            for(int i=0; i<10; i ++) {
                segment.append(("message-" + i).getBytes());
            }

            List<MessageRecord> records = segment.read(5, 1024 * 1024);

            assertThat(records).hasSize(5);
            assertThat(records.get(0).getOffset()).isEqualTo(5L);
        }
    }

    @Test
    void write10kMessages_readBackByRandomOffset() throws IOException {
        try (LogSegment segment = createSegment(0)) {
            // Write 10k messages
            for (int i = 0; i < 10_000; i++) {
                segment.append(("payload-" + i).getBytes());
            }

            // Read from offset 5000 — should get 5000 messages
            List<MessageRecord> records = segment.read(5000, 10 * 1024 * 1024);

            assertThat(records).isNotEmpty();
            assertThat(records.get(0).getOffset()).isEqualTo(5000L);
            assertThat(new String(records.get(0).getPayload())).isEqualTo("payload-5000");
        }
    }

    @Test
    void segmentWithBaseOffset_offsets_startFromBase() throws IOException {
        try (LogSegment segment = createSegment(1000)) {
            long offset = segment.append("first".getBytes());
            assertThat(offset).isEqualTo(1000L);

            List<MessageRecord> records = segment.read(1000, 1024);
            assertThat(records.get(0).getOffset()).isEqualTo(1000L);
        }
    }
}
