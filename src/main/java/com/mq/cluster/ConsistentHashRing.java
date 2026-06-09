package com.mq.cluster;

import com.mq.model.BrokerInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
@Component
public class ConsistentHashRing {
    /**
     * Number of virtual nodes per broker
     * A common practice is to use 100-200 virtual nodes per broker to achieve a good balance.
     */
    private static final int VIRTUAL_NODES_PER_BROKER = 150;

    /**
     * ConcurrentSkipListMap is a thread-safe sorted map implementation.
     * key : position on ring
     * value : BrokerInfo that owns this position
     */
    private final ConcurrentSkipListMap<Integer, BrokerInfo> ring = new ConcurrentSkipListMap<>();

    /**
     * Track with brokers are registered
     * used for clean removal - we need to remove all virtual nodes for a broker when it goes down
     * <p>
     * key : brokerId
     * value : brokerInfo
     */
    private final Map<String, BrokerInfo> registeredBrokers = new ConcurrentSkipListMap<>();

    /**
     * Add broker to ring
     * <p>
     * Create VIRTUAL_NODES_PER_BROKER virtual nodes, each at different hash position
     * <p>
     * hash function : hash(brokerId + "#" + i)
     * "i#" suffix ensures each virtual node lands
     */
    public void addBroker(BrokerInfo broker) {
        registeredBrokers.put(broker.getBrokerId(), broker);

        for (int i = 0; i < VIRTUAL_NODES_PER_BROKER; i++) {
            int hash = hash(broker.getBrokerId() + "#" + i);
            ring.put(hash, broker);
        }
        log.info("Added broker {} with {} virtual nodes to the ring", broker.getBrokerId(), VIRTUAL_NODES_PER_BROKER);
        log.info("Added broker {} to ring. Ring size: {} positions",
                broker, ring.size());
    }

    /**
     * Remove broker from ring
     * Remove all virtual nodes for this broker
     */
    public void removeBroker(String brokerId) {
        BrokerInfo brokerInfo = registeredBrokers.remove(brokerId);

        if (brokerInfo == null) {
            log.warn("Attempted to remove non-existent broker {}", brokerId);
            return;
        }

        for (int i = 0; i < VIRTUAL_NODES_PER_BROKER; i++) {
            int hash = hash(brokerInfo.getBrokerId() + "#" + i);
            ring.remove(hash);
        }

        log.info("Removed broker {} and its virtual nodes from the ring. Ring size: {} positions", brokerInfo, ring.size());
    }

    /**
     * Find which broker is responsible for given partition
     * <p>
     * Algorithm
     * 1. Hash the partition key (e.g. topicName#partitionIndex) to get a position on the ring
     * 2. find the first broker clockwise from the position
     * 3. if no key exists  wrap around to the first broker in the ring
     * <p>
     * this algo take O(log n) time
     */
    public Optional<BrokerInfo> getBrokerForPartition(String partitionKey) {
        if (ring.isEmpty()) return Optional.empty();

        int hash = hash(partitionKey);

        // Find the first position >= hash
        Map.Entry<Integer, BrokerInfo> entry = ring.ceilingEntry(hash);

        // If null, wrap around to the first entry in the ring
        if (entry == null) {
            entry = ring.firstEntry();
        }

        return Optional.of(entry.getValue());
    }

    /**
     * Get all currently registered brokers
     */
    public Collection<BrokerInfo> getAllBrokers() {
        return Collections.unmodifiableCollection(registeredBrokers.values());
    }

    public boolean isEmpty() {
        return ring.isEmpty();
    }

    public int getBrokerCount() {
        return registeredBrokers.size();
    }

    /**
     * Hash function - convert string into integer ring position
     * <p>
     * we implement FNV-1a(fowler-noll-vo), bcz hashCode function may generate same hash-code for different object
     * FNV-1a: generate same output for same input
     * <p>
     * and we use 0x7FFFFFFF to ensure non-negative hash value, since Java's hashCode can return negative numbers.
     */
    int hash(String key) {
        // FNC-1a hash
        int hash = 0x811c9dc5; // FNV offset basis

        for (byte b : key.getBytes()) {
            hash ^= b; // XOR with byte
            hash *= 0x01000193; // FNV prime
        }

        return hash & 0x7FFFFFFF; // ensure non-negative
    }

    public Map<String, Long> getDistribution() {
        Map<String, Long> dist = new HashMap<>();

        ring.values().forEach(
                broker -> dist.merge(broker.getBrokerId(), 1L, Long::sum));

        return dist;
    }
}
