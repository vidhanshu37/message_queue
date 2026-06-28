package com.mq.cluster;

import com.mq.model.BrokerInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class BrokerRegistry {
    private final ConsistentHashRing hashRing;
    private final PartitionMetadata partitionMetadata;

    /**
     * Register broker with cluster
     * <p>
     * 1. Add to hash ring -> create 150 virtual nodes
     * 2. reassign any existing partitions that should now route to this new broker
     * <p>
     * this being called when
     * - broker starts up
     * - broker failover and recover
     */
    public void registerBroker(BrokerInfo broker) {
        log.info("Registered Broker : {}", broker);
        hashRing.addBroker(broker);

        log.info("Ring distribution after adding {} : {}",
                broker.getBrokerId(), hashRing.getDistribution());
    }

    /**
     * Deregister broker from cluster
     * <p>
     * 1. Remove from hash ring
     * 2. remove its partition assignment
     * <p>
     * this being called when
     * - broker shuts down gracefully
     * - broker failover and recover
     */
    public void deregisterBroker(String brokerId) {
        log.info("Deregistering Broker : {}", brokerId);
        hashRing.removeBroker(brokerId);

        partitionMetadata.removeAssignmentsForBroker(brokerId);
    }

    /**
     * Assign partitions of topic to brokers using consistent hashing
     * <p>
     * for each partition :
     * - build partition key - "TopicName-partitionIdx"
     * - ask to ring - "which broker  own this key!"
     * - store the assignment in partition metadata
     */

    public void assignPartitions(String topicName, int partitionCount) {
        if (hashRing.isEmpty()) {
            log.warn("No broker registered - cannot assign partition for {}", topicName);
            return;
        }

        // Step 1: initial assignment via consistent hashing (unchanged)
        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = topicName + "-" + i;
            int finalI = i;
            hashRing.getBrokerForPartition(partitionKey)
                    .ifPresent(brokerInfo -> partitionMetadata.assignLeader(topicName, finalI, brokerInfo));
        }

        // Step 2: rebalance — cap any broker at ceil(partitionCount / brokerCount)
        int brokerCount = hashRing.getBrokerCount();
        int maxPerBroker = (int) Math.ceil((double) partitionCount / brokerCount);

        Map<String, Long> load = partitionMetadata.getTopicAssignment(topicName)
                .values().stream()
                .collect(Collectors.groupingBy(BrokerInfo::getBrokerId, Collectors.counting()));

        // Find overloaded partitions and move them to underloaded brokers
        List<BrokerInfo> allBrokers = new ArrayList<>(hashRing.getAllBrokers());
        Map<Integer, BrokerInfo> assignments = partitionMetadata.getTopicAssignment(topicName);

        for (Map.Entry<Integer, BrokerInfo> entry : assignments.entrySet()) {
            int partIdx = entry.getKey();
            BrokerInfo assigned = entry.getValue();
            long assignedLoad = load.getOrDefault(assigned.getBrokerId(), 0L);

            if (assignedLoad > maxPerBroker) {
                // Find a broker under the max
                Optional<BrokerInfo> underloaded = allBrokers.stream()
                        .filter(b -> load.getOrDefault(b.getBrokerId(), 0L) < maxPerBroker)
                        .findFirst();

                if (underloaded.isPresent()) {
                    BrokerInfo target = underloaded.get();
                    partitionMetadata.assignLeader(topicName, partIdx, target);
                    load.merge(assigned.getBrokerId(), -1L, Long::sum);
                    load.merge(target.getBrokerId(), 1L, Long::sum);
                    log.info("Rebalanced {}-{} from {} to {}", topicName, partIdx,
                            assigned.getBrokerId(), target.getBrokerId());
                }
            }
        }

        log.info("Assigned {} partitions for topic '{}'", partitionCount, topicName);
        partitionMetadata.getTopicAssignment(topicName)
                .forEach((partition, broker) ->
                        log.info(" {}-{} -> {}", topicName, partition, broker.getBrokerId()));
    }

    /**
     * find which broker owns a specific partition
     * used by producer to route message correctly
     */
    public Optional<BrokerInfo> getBrokerForPartition(String topicName, int partitionIdx) {
        return partitionMetadata.getLeader(topicName, partitionIdx);
    }

    public Collection<BrokerInfo> getAllBrokers() {
        return hashRing.getAllBrokers();
    }

    public int getBrokerCount() {
        return hashRing.getBrokerCount();
    }
}
