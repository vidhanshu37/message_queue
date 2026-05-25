package com.mq.service;

import com.mq.storage.LogManager;
import com.mq.storage.MessageRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerLoadTest {

    @TempDir
    Path tempDir;

    /**
     * Test 1: 1000 sequential messages — verify order preserved
     * <p>
     * if offsets are assigned non-sequentially,
     * consumers will process messages out of order.
     */
    @Test
    void sequentialMessages_orderPreserved() throws IOException {
        LogManager logManager = new LogManager(tempDir.toString());
        logManager.initPartition("orders", 0);

        // Write 1000 messages with sequence numbers in payload
        for (int i = 0; i < 1000; i++) {
            logManager.append("orders", 0, ("seq-" + i).getBytes());
        }

        // Read all back
        List<MessageRecord> records = logManager.read("orders", 0, 0, 10 * 1024 * 1024);

        assertThat(records).hasSize(1000);

        // Verify every offset is sequential
        for (int i = 0; i < records.size(); i++) {
            assertThat(records.get(i).getOffset())
                    .as("Offset at position %d should be %d", i, i)
                    .isEqualTo(i);

            // Verify payload matches position
            assertThat(new String(records.get(i).getPayload()))
                    .isEqualTo("seq-" + i);
        }
    }

    /**
     * Test 2: 4 threads producing concurrently — verify no duplicate offsets
     * <p>
     * Why this matters: concurrent producers are the real world.
     * If our synchronized append has a bug, two messages get the same offset.
     * A Set of offsets will have fewer elements than total messages if duplicates exist.
     */
    @Test
    void concurrentProducers_noDuplicateOffsets() throws Exception {
        LogManager logManager = new LogManager(tempDir.toString());
        logManager.initPartition("orders", 0);

        int threadsCount = 4;
        int messagesPerThread = 250; // 4 * 250 = 1000 total
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        List<Future<List<Long>>> futures = new ArrayList<>();

        // Each thread produces 250 messages and collects returned offsets
        for (int t = 0; t < threadsCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                List<Long> offsets = new ArrayList<>();
                for (int i = 0; i < messagesPerThread; i++) {
                    long offset = logManager.append("orders", 0,
                            ("thread-" + threadId + "-msg-" + i).getBytes());
                    offsets.add(offset);
                }
                return offsets;
            }));
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Collect all offsets from all threads
        List<Long> allOffsets = new ArrayList<>();
        for (Future<List<Long>> future : futures) {
            allOffsets.addAll(future.get());
        }

        // Total messages written
        assertThat(allOffsets).hasSize(1000);

        // If any two messages got the same offset, the Set will be smaller
        Set<Long> uniqueOffsets = allOffsets.stream().collect(Collectors.toSet());
        assertThat(uniqueOffsets)
                .as("Every message must have a unique offset — no duplicates allowed")
                .hasSize(1000);

        // Offsets should span 0 to 999
        assertThat(uniqueOffsets).contains(0L, 999L);

    }
}
