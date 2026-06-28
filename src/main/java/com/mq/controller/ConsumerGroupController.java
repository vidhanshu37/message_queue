package com.mq.controller;

import com.mq.cluster.BrokerRegistry;
import com.mq.consumerGroup.ConsumerGroupCoordinator;
import com.mq.dto.request.HeartbeatGroupRequest;
import com.mq.dto.request.JoinGroupRequest;
import com.mq.dto.request.LeaveGroupRequest;
import com.mq.dto.response.*;
import com.mq.raft.RaftNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/api/v1/groups")
@RequiredArgsConstructor
@RestController
public class ConsumerGroupController {
    private final ConsumerGroupCoordinator coordinator;
    private final RaftNode raftNode;
    private final BrokerRegistry brokerRegistry;

    @PostMapping("/{groupId}/join")
    public ResponseEntity<?> joinGroup(@PathVariable String groupId, @RequestBody JoinGroupRequest request) {

        request.setGroupId(groupId);

        ResponseEntity<?> leaderCheck = redirectIfNotLeader("/api/v1/groups/" + groupId + "/join");

        if(leaderCheck != null)
            return leaderCheck;

        log.info("Join request: group={}, consumer={}, topic={}",
                groupId, request.getConsumerId(), request.getTopicName());

        JoinGroupResponse response = coordinator.joinGroup(request);
        return ResponseEntity.ok(response);
    }

    // heartbeat
    @PostMapping("/{groupId}/heartbeat")
    public ResponseEntity<?> heartbeat(@PathVariable String groupId, @RequestBody HeartbeatGroupRequest request) {

        request.setGroupId(groupId);

        ResponseEntity<?> leaderCheck = redirectIfNotLeader("/api/v1/groups/" + groupId + "/heartbeat");
        if (leaderCheck != null) return leaderCheck;

        HeartbeatGroupResponse response = coordinator.heartbeat(request);

        return ResponseEntity.ok(response);
    }

    // leave
    @PostMapping("/{groupId}/leave")
    public ResponseEntity<?> leaveGroup(@PathVariable String groupId, @RequestBody LeaveGroupRequest request) {

        request.setGroupId(groupId);

        ResponseEntity<?> leaderCheck = redirectIfNotLeader("/api/v1/groups/" + groupId + "/leave");
        if (leaderCheck != null) return leaderCheck;

        log.info("Leave request: group={}, consumer={}", groupId, request.getConsumerId());

        LeaveGroupResponse response = coordinator.leaveGroup(request);
        HttpStatus status = response.isSuccess() ? HttpStatus.OK : HttpStatus.NOT_FOUND;
        return ResponseEntity.status(status).body(response);
    }

    // assignment
    @GetMapping("/{groupId}/assignment")
    public ResponseEntity<AssignmentResponse> getAssignment(
            @PathVariable String groupId,
            @RequestParam String consumerId) {

        AssignmentResponse response = coordinator.getAssignment(groupId, consumerId);

        // 404 if group or consumer not found
        if (response.getGeneration() == -1) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/{groupId}/status")
    public ResponseEntity<?> getGroupStatus(@PathVariable String groupId) {
        GroupStatusResponse response = coordinator.getGroupStatus(groupId);

        if (response.getState() == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Group not found: " + groupId));
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping
    public ResponseEntity<?> listGroups() {
        List<String> groups = coordinator.listGroups();
        return ResponseEntity.ok(Map.of(
                "groups", groups,
                "count",  groups.size()
        ));
    }

    private ResponseEntity<?> redirectIfNotLeader(String path) {
        if (raftNode.isLeader()) return null;

        String leaderId = raftNode.getCurrentLeaderId();
        if (leaderId == null) {
            return ResponseEntity
                    .status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "No leader elected yet — try again shortly"));
        }

        return brokerRegistry.getAllBrokers().stream()
                .filter(b -> b.getBrokerId().equals(leaderId))
                .findFirst()
                .map(leader -> {
                    String redirectUrl = leader.baseUrl() + path;
                    log.info("Not leader — redirecting group request to: {}", redirectUrl);

                    HttpHeaders headers = new HttpHeaders();
                    headers.add("Location", redirectUrl);

                    return ResponseEntity
                            .status(HttpStatus.TEMPORARY_REDIRECT)
                            .headers(headers)
                            .<Object>body(null);
                })
                .orElseGet(() ->
                        ResponseEntity
                                .status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body(Map.of("error", "Leader " + leaderId + " not found in registry"))
                );
    }

}
