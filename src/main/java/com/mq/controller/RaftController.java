package com.mq.controller;

import com.mq.dto.request.HeartbeatRequest;
import com.mq.dto.request.RequestVoteRequest;
import com.mq.dto.response.HeartbeatResponse;
import com.mq.dto.response.RequestVoteResponse;
import com.mq.raft.RaftNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/raft")
@RequiredArgsConstructor
public class RaftController {
    private final RaftNode raftNode;


   //  RequestVote RPC — called by candidates during election
    @PostMapping("/vote")
    public ResponseEntity<RequestVoteResponse> requestVote(@RequestBody RequestVoteRequest request) {
        RequestVoteResponse response = raftNode.handleRequestVote(request);

        return ResponseEntity.ok(response);
    }

    // Heartbeat RPC — called by leader every 50ms
    @PostMapping("/heartbeat")
    public ResponseEntity<HeartbeatResponse> heartbeat(@RequestBody HeartbeatRequest request) {
        HeartbeatResponse response = raftNode.handleHeartBeat(request);

        return ResponseEntity.ok(response);
    }

    // get current raft state of this node
    @GetMapping("/state")
    public ResponseEntity<?> getState() {
        return ResponseEntity.ok(Map.of(
                "brokerId", raftNode.getCurrentLeaderId() != null
                    ? raftNode.getCurrentLeaderId() : "unknown",
                "state", raftNode.getState(),
                "term", raftNode.getCurrentTerm(),
                "isLeader", raftNode.isLeader(),
                "currentLeader", raftNode.getCurrentLeaderId() != null
                ? raftNode.getCurrentLeaderId() : "none"
        ));
    }
}
