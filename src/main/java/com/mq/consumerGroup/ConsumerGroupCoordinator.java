package com.mq.consumerGroup;

import com.fasterxml.jackson.databind.ObjectReader;
import com.mq.dto.request.HeartbeatGroupRequest;
import com.mq.dto.request.JoinGroupRequest;
import com.mq.dto.request.LeaveGroupRequest;
import com.mq.dto.response.*;
import com.mq.raft.RaftNode;
import com.mq.repository.TopicRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerGroupCoordinator {
    private final RaftNode raftNode;
    private final TopicRepository topicRepository;

    // State
    private final ConcurrentHashMap<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> groupLocks = new ConcurrentHashMap<>();

    // background task
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cg-coordination");
                t.setDaemon(true);
                return t;
            });

    @PostConstruct
    public void start() {
        scheduler.scheduleAtFixedRate(
                this::evictDeadMembers, 5, 5, TimeUnit.SECONDS);
        log.info("ConsumerGroupCoordinator started");
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdown();
        log.info("ConsumerGroupCoordinator stopped");
    }

    /// JOIN - consumer wants to enter group
    /*
    Steps:
       1. Validate: only leader handles this
       2. Get or create the ConsumerGroup
       3. Add / refresh the member
       4. Trigger rebalance (synchronous Range assignment)
       5. Return assignment to caller
     */
    public JoinGroupResponse joinGroup(JoinGroupRequest request) {
        if (!raftNode.isLeader()) {
            return new JoinGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    -1, GroupState.PREPARING_REBALANCE, List.of(),
                    request.getTopicName(),
                    "Not a leader - redirect to : " + raftNode.getCurrentLeaderId()
            );
        }

        // validate topic exists
        boolean topicExists = topicRepository.findByName(request.getTopicName()).isPresent();
        if (!topicExists) {
            return new JoinGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    -1, GroupState.EMPTY, List.of(),
                    request.getTopicName(),
                    "Topic not found: " + request.getTopicName());
        }

        Object lock = groupLocks.computeIfAbsent(request.getGroupId(), k -> new Object());

        synchronized (lock) {
            // get or create group
            ConsumerGroup group = groups.computeIfAbsent(request.getGroupId(), id -> new ConsumerGroup(id));

            // Set partition count from db
            topicRepository.findByName(request.getTopicName()).ifPresent(t -> group.setTotalPartitions(t.getPartitionCount()));

            // add or refresh member
            if (group.hasMember(request.getConsumerId())) {
                // Rejoin after rebalance — refresh heartbeat
                group.getMember(request.getConsumerId()).refreshHeartBeat();
                log.info("Group {}: consumer {} re-joined", request.getGroupId(), request.getConsumerId());
            } else {
                // new member
                ConsumerGroupMember member = new ConsumerGroupMember(
                        request.getConsumerId(),
                        request.getTopicName(),
                        request.getSessionTimeoutMs());
                group.addMember(member);
            }

            // rebalance on join
            group.transitionTo(GroupState.PREPARING_REBALANCE);
            performRebalance(group);

            // Update joined generation for this member
            ConsumerGroupMember member = group.getMember(request.getConsumerId());
            if (member != null) {
                member.setJoinedGeneration(group.getGeneration());
            }

            List<Integer> assigned = group.getAssignmentFor(request.getConsumerId());

            log.info("Group {}: consumer {} joined. Generation={}. Assigned partitions={}",
                    request.getGroupId(), request.getConsumerId(),
                    group.getGeneration(), assigned);

            return new JoinGroupResponse(
                    group.getGroupId(),
                    request.getConsumerId(),
                    group.getGeneration(),
                    group.getState(),
                    assigned,
                    request.getTopicName(),
                    "Joined successfully");
        }

    }

    ///  Heartbeat
    public HeartbeatGroupResponse heartbeat(HeartbeatGroupRequest request) {
        if (!raftNode.isLeader()) {
            return new HeartbeatGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    -1, false, false, "Not the leader"
            );
        }

        ConsumerGroup group = groups.get(request.getGroupId());

        if(group == null || !group.hasMember(request.getConsumerId())) {
            log.warn("Heartbeat from unknown consumer {}/{}", request.getGroupId(), request.getConsumerId());
            return new HeartbeatGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    -1, false, false,
                    "Consumer not found — please re-join");
        }

        // refresh
        group.getMember(request.getConsumerId()).refreshHeartBeat();

        // generation check
        if(request.getGeneration() != group.getGeneration()) {
            log.info("Group {}: consumer {} has stale generation {} (current={}). Rebalance required.",
                    request.getGroupId(), request.getConsumerId(),
                    request.getGeneration(), group.getGeneration());
            return new HeartbeatGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    group.getGeneration(), true, true,
                    "Rebalance occurred — please re-join to get updated assignment");
        }

        // All good
        return new HeartbeatGroupResponse(
                request.getGroupId(), request.getConsumerId(),
                group.getGeneration(), true,
                group.getState() == GroupState.PREPARING_REBALANCE,
                "OK");
    }

    /// Leave - consumer shutdown
    public LeaveGroupResponse leaveGroup(LeaveGroupRequest request) {
        if (!raftNode.isLeader()) {
            return new LeaveGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    false, "Not the leader");
        }

        Object lock = groupLocks.computeIfAbsent(request.getGroupId(), k -> new Object());

        synchronized (lock) {
            ConsumerGroup group = groups.get(request.getGroupId());

            if (group == null || !group.hasMember(request.getConsumerId())) {
                return new LeaveGroupResponse(
                        request.getGroupId(), request.getConsumerId(),
                        false, "Consumer not found in group");
            }

            group.removeMember(request.getConsumerId());
            log.info("Group {}: consumer {} left gracefully. Remaining: {}",
                    request.getGroupId(), request.getConsumerId(), group.getMemberCount());

            if (group.isEmpty()) {
                group.transitionTo(GroupState.EMPTY);
                group.clearAssignments();
                log.info("Group {} is now EMPTY", request.getGroupId());
            } else {
                // Rebalance remaining members
                group.transitionTo(GroupState.PREPARING_REBALANCE);
                performRebalance(group);
            }

            return new LeaveGroupResponse(
                    request.getGroupId(), request.getConsumerId(),
                    true, "Left group successfully. Rebalance triggered.");
        }
    }

    ///  Get assignment
    public AssignmentResponse getAssignment(String groupId, String consumerId) {
        ConsumerGroup group = groups.get(groupId);

        if (group == null) {
            return new AssignmentResponse(groupId, consumerId, null,
                    -1, GroupState.EMPTY, List.of(), "Group not found");
        }

        if (!group.hasMember(consumerId)) {
            return new AssignmentResponse(groupId, consumerId, group.getTopicName(),
                    -1, group.getState(), List.of(), "Consumer not found — please join first");
        }

        List<Integer> partitions = group.getAssignmentFor(consumerId);

        return new AssignmentResponse(
                groupId, consumerId,
                group.getTopicName(),
                group.getGeneration(),
                group.getState(),
                partitions,
                group.getState() == GroupState.STABLE
                        ? "Assignment ready"
                        : "Rebalance in progress — please retry shortly");
    }

    ///  Get status
    public GroupStatusResponse getGroupStatus(String groupId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            return new GroupStatusResponse(groupId, GroupState.EMPTY, 0,
                    null, 0, 0, Map.of(), Map.of(), "Group not found");
        }

        Map<String, List<Integer>> assignments = group.getAllMembers().stream()
                .collect(Collectors.toMap(
                        ConsumerGroupMember::getConsumerId,
                        m -> group.getAssignmentFor(m.getConsumerId())));

        Map<String, Long> heartbeatAge = group.getAllMembers().stream()
                .collect(Collectors.toMap(
                        ConsumerGroupMember::getConsumerId,
                        ConsumerGroupMember::millisSinceHeartbeat));

        return new GroupStatusResponse(
                groupId,
                group.getState(),
                group.getGeneration(),
                group.getTopicName(),
                group.getTotalPartitions(),
                group.getMemberCount(),
                assignments,
                heartbeatAge,
                "OK");
    }

    public List<String> listGroups() {
        return new ArrayList<>(groups.keySet());
    }

    ///  Rebalance
    private void performRebalance(ConsumerGroup group) {
        int totalPartitions = group.getTotalPartitions();
        int totalConsumers  = group.getMemberCount();

        if (totalConsumers == 0) {
            group.clearAssignments();
            group.transitionTo(GroupState.EMPTY);
            return;
        }

        List<String> sortedConsumers = group.getAllMembers().stream()
                .map(ConsumerGroupMember::getConsumerId)
                .sorted()
                .toList();

        group.clearAssignments();

        // Range algorithm
        int base   = totalPartitions / totalConsumers;
        int extras = totalPartitions % totalConsumers;

        int partitionStart = 0;

        for (int i = 0; i < sortedConsumers.size(); i++) {
            String consumerId = sortedConsumers.get(i);

            int count = base + (i < extras ? 1 : 0);

            List<Integer> assigned = new ArrayList<>();
            for (int p = partitionStart; p < partitionStart + count; p++) {
                assigned.add(p);
            }

            group.setAssignment(consumerId, assigned);
            partitionStart += count;

            log.info("Group {}: assigned {} → partitions {}",
                    group.getGroupId(), consumerId, assigned);
        }

        group.incrementGeneration();
        group.transitionTo(GroupState.STABLE);

        log.info("Group {} rebalance complete. Generation={}, {} consumers, {} partitions",
                group.getGroupId(), group.getGeneration(),
                totalConsumers, totalPartitions);
    }

    private void evictDeadMembers() {
        if (!raftNode.isLeader()) return;

        for (Map.Entry<String, ConsumerGroup> entry : groups.entrySet()) {
            String groupId = entry.getKey();
            ConsumerGroup group = entry.getValue();

            List<String> dead = group.getTimedOutMembers();
            if (dead.isEmpty()) continue;

            Object lock = groupLocks.computeIfAbsent(groupId, k -> new Object());
            synchronized (lock) {
                dead = group.getTimedOutMembers();
                if (dead.isEmpty()) continue;

                for (String consumerId : dead) {
                    log.warn("Group {}: consumer {} timed out — evicting", groupId, consumerId);
                    group.removeMember(consumerId);
                }

                if (group.isEmpty()) {
                    group.transitionTo(GroupState.EMPTY);
                    group.clearAssignments();
                    log.info("Group {} is now EMPTY after eviction", groupId);
                } else {
                    group.transitionTo(GroupState.PREPARING_REBALANCE);
                    performRebalance(group);
                    log.info("Group {} rebalanced after eviction. New generation={}",
                            groupId, group.getGeneration());
                }
            }
        }
    }
}
