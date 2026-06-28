package com.mq.consumerGroup;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Getter
public class ConsumerGroup {
    private final String groupId;
    private volatile GroupState state;

    private volatile int generation;

    private final Map<String, ConsumerGroupMember> members;

    private final Map<String, List<Integer>> assignments;

    private volatile String topicName;

    private volatile int totalPartitions;

    public ConsumerGroup(String groupId) {
        this.groupId = groupId;
        this.state = GroupState.EMPTY;
        this.generation = 0;
        this.members = new ConcurrentHashMap<>();
        this.assignments = new ConcurrentHashMap<>();
    }

    // member management
    public void addMember(ConsumerGroupMember member) {
        members.put(member.getConsumerId(), member);

        if(topicName == null) {
            topicName = member.getTopicName();
        }
        log.info("Group {}: member {} joined. Total members: {}", groupId, member.getConsumerId(), members.size());
    }

    public void removeMember(String consumerId) {
        ConsumerGroupMember removed = members.remove(consumerId);
        assignments.remove(consumerId);
        if (removed != null) {
            log.info("Group {}: member {} removed. Remaining members: {}", groupId, consumerId, members.size());
        }
    }

    public boolean hasMember(String consumerId) {
        return members.containsKey(consumerId);
    }

    public ConsumerGroupMember getMember(String consumerId) {
        return members.get(consumerId);
    }

    public Collection<ConsumerGroupMember> getAllMembers() {
        return Collections.unmodifiableCollection(members.values());
    }

    public int getMemberCount() {
        return members.size();
    }

    // state transitions
    public void transitionTo(GroupState newState) {
        log.info("Group {} state: {} → {}", groupId, this.state, newState);
        this.state = newState;
    }

    public void incrementGeneration() {
        this.generation++;
        log.info("Group {} generation incremented to {}", groupId, this.generation);
    }

    // Assignment management
    public void setAssignment(String consumerId, List<Integer> partitions) {
        assignments.put(consumerId, partitions);
        ConsumerGroupMember member = members.get(consumerId);
        if (member != null) {
            member.setAssignedPartitions(partitions);
        }
    }

    public List<Integer> getAssignmentFor(String consumerId) {
        return assignments.getOrDefault(consumerId, List.of());
    }

    public void clearAssignments() {
        assignments.clear();
        members.values().forEach(m -> m.setAssignedPartitions(List.of()));
    }

    // Partition config
    public void setTotalPartitions(int totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    // helper
    public List<String> getTimedOutMembers() {
        return members.values().stream()
                .filter(ConsumerGroupMember::isTimeOut)
                .map(ConsumerGroupMember::getConsumerId)
                .toList();
    }

    public boolean isEmpty() {
        return members.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("ConsumerGroup{id=%s, state=%s, gen=%d, members=%s}",
                groupId, state, generation, members.keySet());
    }
}
