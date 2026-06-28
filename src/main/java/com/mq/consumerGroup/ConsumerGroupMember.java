package com.mq.consumerGroup;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

@Slf4j
@Getter
@Setter
public class ConsumerGroupMember {
    private final String consumerId;
    private final String topicName;

    // waiting time how long co-ordinator waits for heartbeat
    private final long sessionTimeoutMs;

    private List<Integer> assignedPartitions;

    private Instant lastHeartBeat;

    // used to detect stale member
    private int joinedGeneration;


    public ConsumerGroupMember(String consumerId, String topicName, long sessionTimeoutMs) {
        this.consumerId = consumerId;
        this.topicName = topicName;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.assignedPartitions = List.of();
        this.lastHeartBeat = Instant.now();
    }

    public void refreshHeartBeat() {
        this.lastHeartBeat = Instant.now();
        log.debug("Heartbeat refresh for consumer {}", consumerId);
    }

    public boolean isTimeOut() {
        long elapsed = Instant.now().toEpochMilli() - lastHeartBeat.toEpochMilli();
        return elapsed > sessionTimeoutMs;
    }

    public long millisSinceHeartbeat() {
        return Instant.now().toEpochMilli() - lastHeartBeat.toEpochMilli();
    }

    @Override
    public String toString() {
        return String.format("ConsumerGroupMember{id=%s, topic=%s, partitions=%s, lastHb=%s}",
                consumerId, topicName, assignedPartitions, lastHeartBeat);
    }


}
