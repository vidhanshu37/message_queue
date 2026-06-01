package com.mq.cluster;

import com.mq.model.BrokerInfo;
import jakarta.persistence.criteria.CriteriaBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class PartitionMetadata {

    /**
     * routing table
     * <p>
     * key = "topicName-partitionIdx" -> "orders-0"
     * value = BrokerInfo responsible for this partition
     */
    private final Map<String, BrokerInfo> partitionLeaders = new ConcurrentHashMap<>();

    /**
     * Assign broker as a leader for partition
     * Called during topic creation and after rebalancing
     */
    public void assignLeader(String topicName, int partitionIdx, BrokerInfo broker) {
        String key = buildKey(topicName, partitionIdx);
        partitionLeaders.put(key, broker);

        log.info("Assigned {}-{} to broker {}", topicName, partitionIdx, broker.getBrokerId());
    }

    /**
     * Get broker responsible for partition, return empty if partition has not been assigned
     */
    public Optional<BrokerInfo> getLeader(String topicName, int partitionIdx) {
        return Optional.ofNullable(partitionLeaders.get(buildKey(topicName, partitionIdx)));
    }


    /**
     * Get All partition assignment for topic
     * It Used by producer to know where to send each partition's message
     */
    public Map<Integer, BrokerInfo> getTopicAssignment(String topicName) {
        Map<Integer, BrokerInfo> assignments = new HashMap<>();

        partitionLeaders.forEach((key, broker) -> {
            if (key.startsWith(topicName + "-")) {
                // Extract partition index
                String indexStr = key.substring(topicName.length() + 1);

                try {
                    assignments.put(Integer.parseInt(indexStr), broker);
                } catch (NumberFormatException e) {
                    log.error("Invalid partition key format: {}", key);
                }
            }
        });

        return assignments;
    }

    /**
     * Remove all partition assignment for a topic, called when broker is deleted
     * and need to be reassigned to remaining brokers
     */
    public List<String> removeAssignmentsForBroker(String brokerId) {
        List<String> removedKeys = new ArrayList<>();

        partitionLeaders.entrySet().removeIf(entry -> {
            if(entry.getValue().getBrokerId().equals(brokerId)) {
                removedKeys.add(entry.getKey());
                return true;
            }
            return false;
        });

        log.info("Removed partition assignments for broker {}: {}", brokerId, removedKeys);
        return removedKeys;
    }

    /**
     * Get full routing table
     */
    public Map<String, BrokerInfo> getAllAssignments() {
        return Collections.unmodifiableMap(partitionLeaders);
    }




    private String buildKey(String topicName, int partitionIdx) {
        return topicName + "-" + partitionIdx;
    }

}
