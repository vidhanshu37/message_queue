package com.mq.cluster;

import com.mq.model.BrokerInfo;
import org.hibernate.dialect.lock.OptimisticEntityLockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsistentHashRingTest {
    private ConsistentHashRing ring;

    @BeforeEach
    void stepUp() {
        ring = new ConsistentHashRing();
    }

    @Test
    void addBroker_partitionRoutesToIt() {
        BrokerInfo broker = new BrokerInfo("broker-1", "localhost", 8080);
        ring.addBroker(broker);

        Optional<BrokerInfo> result = ring.getBrokerForPartition("orders-0");

        assertThat(result).isPresent();
        assertThat(result.get().getBrokerId()).isEqualTo("broker-1");
    }

    @Test
    void sameBrokerCount_sameRoutingDecision() {
        BrokerInfo b1 = new BrokerInfo("broker-1", "localhost", 8080);
        BrokerInfo b2 = new BrokerInfo("broker-2", "localhost", 8081);
        BrokerInfo b3 = new BrokerInfo("broker-3", "localhost", 8082);

        ring.addBroker(b1);
        ring.addBroker(b2);
        ring.addBroker(b3);

        // Record initial assignments for 12 partitions
        Map<String, String> initial = new HashMap<>();
        for (int i = 0; i < 12; i++) {
            String partition = "orders-" + i;
            ring.getBrokerForPartition(partition)
                    .ifPresent(b -> initial.put(partition, b.getBrokerId()));
        }

        // Ask again - must get identical results
        for (int i = 0; i < 12; i++) {
            String partition = "orders-" + i;
            Optional<BrokerInfo> broker = ring.getBrokerForPartition(partition);

            assertThat(broker).isPresent();
            assertThat(broker.get().getBrokerId())
                    .as("Partition %s should route to same broker on repeated calls", partition)
                    .isEqualTo(initial.get(partition));
        }
    }

    @Test
    void removeBroker_itsPartitionsRedistribute() {
        BrokerInfo b1 = new BrokerInfo("broker-1", "localhost", 8080);
        BrokerInfo b2 = new BrokerInfo("broker-2", "localhost", 8081);
        BrokerInfo b3 = new BrokerInfo("broker-3", "localhost", 8082);

        ring.addBroker(b1);
        ring.addBroker(b2);
        ring.addBroker(b3);

        // Remove broker-2
        ring.removeBroker("broker-2");

        // All 12 partitions should still route somewhere
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 12; i++) {
            Optional<BrokerInfo> b = ring.getBrokerForPartition("orders-" + i);

            assertThat(b).isPresent();
            assertThat(b.get().getBrokerId())
                    .as("No partition should route to removed broker-2")
                    .isNotEqualTo("broker-2");

            counts.merge(b.get().getBrokerId(), 1, Integer::sum);
        }

        // broker-1 and broker-3 together must cover all 12
        int total = counts.getOrDefault("broker-1", 0)
                + counts.getOrDefault("broker-3", 0);
        assertThat(total)
                .as("All partitions must be assigned to remaining brokers")
                .isEqualTo(12);
    }


}
