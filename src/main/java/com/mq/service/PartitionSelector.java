package com.mq.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class PartitionSelector {
    // One counter per topic for round-robin
    // AtomicInteger because multiple producer threads call this simultaneously
    private final ConcurrentHashMap<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();

    /**
     * Select partition for given topic and optional key
     *
     * if key provided -> hash(key) % partitionCount (sticky routing)
     *  no key  -> round-robin (load-balancing)
     * @param topicName      the topic name
     * @param key            optional message key
     * @param partitionCount total number of partitions for this topic
     * @return partition index (0 to partitionCount-1)
     */
    public int selectPartition(String topicName, String key, int partitionCount) {
        if(key != null && !key.isBlank()) {
            return keyBasedPartition(key, partitionCount);
        }

        return roundRobinPartition(topicName, partitionCount);
    }

    /**
     *  Round-robin partition selection
     *
     * AtomicInteger.getAndIncrement() is a single atomic CPU instruction.
     * No synchronized block needed — faster under high concurrency.
     *
     * Thread-safe — two threads can't both create the counter simultaneously
     *
     * @param topicName
     * @param partitionCount
     * @return
     */

    private int roundRobinPartition(String topicName, int partitionCount) {
        AtomicInteger counter = roundRobinCounters.computeIfAbsent(topicName, k -> new AtomicInteger(0));

        // % partition count so keep in valid range
        int partition = counter.getAndIncrement() % partitionCount;
        log.debug("Round-robin partition selection: topic='{}' -> partition {}", topicName, partition);
        return partition;
    }

    /**
     * Hash-based partition selection
     *
     * Math.abs() -> hashCode() can return negative numbers.
     * so we use a bit masking -> 0x7FFFFFFF instead of using Math.abs()
     *
     * @param key
     * @param partitionCount
     * @return
     */
    private int keyBasedPartition(String key, int partitionCount) {
        int partition = (key.hashCode() & 0x7FFFFFFF) % partitionCount;
        log.debug("Key-based partition selection: key='{}' -> partition {}", key, partition);
        return partition;
    }

}
