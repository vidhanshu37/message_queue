package com.mq.raft;

import com.mq.cluster.BrokerRegistry;
import com.mq.cluster.PartitionMetadata;
import com.mq.dto.request.HeartbeatRequest;
import com.mq.dto.request.RequestVoteRequest;
import com.mq.dto.response.HeartbeatResponse;
import com.mq.dto.response.RequestVoteResponse;
import com.mq.model.BrokerInfo;
import com.mq.repository.TopicRepository;
import com.mq.storage.LogManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class RaftNode {

    // ── Persistent Raft state ────────────────────────────────────────────────
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;

    // ── Volatile Raft state ──────────────────────────────────────────────────
    @Getter private volatile RaftState state = RaftState.FOLLOWER;
    @Getter private volatile String currentLeaderId = null;

    // ── Election timing ──────────────────────────────────────────────────────
    private volatile long lastHeartbeatTime;
    private volatile long electionDeadline;

    private static final int ELECTION_TIMEOUT_MIN_MS = 300;
    private static final int ELECTION_TIMEOUT_MAX_MS = 700;
    private static final int HEARTBEAT_INTERVAL_MS   = 50;

    // ── Dependencies ─────────────────────────────────────────────────────────
    private final BrokerRegistry    brokerRegistry;
    private final PartitionMetadata partitionMetadata;
    private final LogManager        logManager;
    private final TopicRepository   topicRepository;
    private final RestTemplate      restTemplate;
    private final Random            random = new Random();

    @Value("${broker.id:broker-1}")
    private String currentBrokerId;

    // ── Background threads ───────────────────────────────────────────────────
    private final ScheduledExecutorService electionExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-election");
                t.setDaemon(true);
                return t;
            });

    private final ScheduledExecutorService heartbeatExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-heartbeat");
                t.setDaemon(true);
                return t;
            });

    public RaftNode(BrokerRegistry brokerRegistry,
                    PartitionMetadata partitionMetadata,
                    LogManager logManager,
                    TopicRepository topicRepository,
                    RestTemplate restTemplate) {
        this.brokerRegistry    = brokerRegistry;
        this.partitionMetadata = partitionMetadata;
        this.logManager        = logManager;
        this.topicRepository   = topicRepository;
        this.restTemplate      = restTemplate;
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    @PostConstruct
    public void start() {
        log.info("Starting RaftNode with brokerId {} in state {}", currentBrokerId, state);
        resetElectionDeadline(2000); // 2 s startup grace period

        electionExecutor.scheduleAtFixedRate(
                this::checkElectionTimeout, 2000, 10, TimeUnit.MILLISECONDS);

        heartbeatExecutor.scheduleAtFixedRate(
                this::maybeSendHeartbeats,
                2000 + HEARTBEAT_INTERVAL_MS,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        log.info("RaftNode started. Initial state: {}, initial term: {}, initial leader: {}",
                state, currentTerm.get(), currentLeaderId);
    }

    @PreDestroy
    public void stop() {
        electionExecutor.shutdownNow();
        heartbeatExecutor.shutdownNow();
        log.info("RaftNode stopped");
    }

    // ── Election timeout ─────────────────────────────────────────────────────

    private void checkElectionTimeout() {
        if (state == RaftState.LEADER) return;
        long now = System.currentTimeMillis();
        if (now >= electionDeadline) {
            long elapsed = now - lastHeartbeatTime;
            log.info("Election timeout! No heartbeat for {}ms. Starting election. Term: {}",
                    elapsed, currentTerm.get());
            startElection();
        }
    }

    /** Reset both the heartbeat timestamp and the fixed election deadline. */
    private void resetElectionDeadline() {
        long now = System.currentTimeMillis();
        lastHeartbeatTime = now;
        long timeout = ELECTION_TIMEOUT_MIN_MS
                + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        electionDeadline = now + timeout;
    }

    private void resetElectionDeadline(int extraDelayMs) {
        long now = System.currentTimeMillis();
        lastHeartbeatTime = now;
        long timeout = ELECTION_TIMEOUT_MIN_MS
                + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        electionDeadline = now + extraDelayMs + timeout;
    }

    // ── Election logic ───────────────────────────────────────────────────────

    /**
     * Three-phase election:
     *   Phase 1 (locked)   – become CANDIDATE, bump term, self-vote
     *   Phase 2 (no lock)  – collect votes via HTTP (lock-free so heartbeats can still land)
     *   Phase 3 (locked)   – evaluate result; become LEADER or revert to FOLLOWER
     */
    private void startElection() {
        // ── Phase 1 ──────────────────────────────────────────────────────────
        final int newTerm;
        final List<BrokerInfo> peers;
        final int majority;

        synchronized (this) {
            if (state == RaftState.LEADER)    return;
            if (state == RaftState.CANDIDATE) return; // already mid-election

            state        = RaftState.CANDIDATE;
            newTerm      = currentTerm.incrementAndGet();
            votedFor     = currentBrokerId;
            currentLeaderId = null;
            resetElectionDeadline();

            log.info("Started election for term {}. Voted for self.", newTerm);

            peers = brokerRegistry.getAllBrokers().stream()
                    .filter(b -> !b.getBrokerId().equals(currentBrokerId))
                    .toList();

            if (peers.isEmpty()) {
                becomeLeader(newTerm);
                return;
            }

            int totalNodes = peers.size() + 1;
            majority = (totalNodes / 2) + 1;
        }

        // ── Phase 2 (lock-free) ───────────────────────────────────────────────
        AtomicInteger voteCount = new AtomicInteger(1); // self-vote

        List<CompletableFuture<Void>> futures = peers.stream()
                .map(peer -> requestVoteFrom(peer, newTerm, voteCount, majority))
                .toList();

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(ELECTION_TIMEOUT_MAX_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Vote collection timed out for term {}", newTerm);
        } catch (Exception e) {
            log.warn("Vote collection error for term {}: {}", newTerm, e.getMessage());
        }

        // ── Phase 3 ──────────────────────────────────────────────────────────
        synchronized (this) {
            if (state != RaftState.CANDIDATE) {
                log.info("Election for term {} cancelled — state is now {}", newTerm, state);
                return;
            }
            if (currentTerm.get() != newTerm) {
                log.info("Election for term {} stale — current term is {}", newTerm, currentTerm.get());
                state = RaftState.FOLLOWER;
                resetElectionDeadline();
                return;
            }
            if (voteCount.get() >= majority) {
                becomeLeader(newTerm);
            } else {
                log.info("Lost election for term {}. Got {}/{} votes.", newTerm, voteCount.get(), majority);
                state = RaftState.FOLLOWER;
                resetElectionDeadline();
            }
        }
    }

    private CompletableFuture<Void> requestVoteFrom(BrokerInfo peer, int term,
                                                    AtomicInteger voteCount, int majority) {
        return CompletableFuture.runAsync(() -> {
            try {
                RequestVoteRequest request = new RequestVoteRequest(
                        term, currentBrokerId, getLatestLogOffset(), "cluster", -1);

                RequestVoteResponse response = restTemplate.postForObject(
                        peer.baseUrl() + "/api/v1/raft/vote",
                        request, RequestVoteResponse.class);

                if (response == null) return;

                if (response.getTerm() > currentTerm.get()) {
                    becomeFollower(response.getTerm(), null);
                    return;
                }

                if (response.isVoteGranted()) {
                    int votes = voteCount.incrementAndGet();
                    log.info("Received vote from {} for term {}. Total votes: {}/{}",
                            peer.getBrokerId(), term, votes, majority);
                }
            } catch (Exception e) {
                log.warn("Error requesting vote from {}: {}", peer.getBrokerId(), e.getMessage());
            }
        });
    }

    // ── Vote-granting (receiver side) ────────────────────────────────────────

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        int candidateTerm = request.getTerm();
        String candidateId = request.getCandidateId();

        log.info("Received RequestVote from {} for term {}. Current term: {}, votedFor: {}, state: {}",
                candidateId, candidateTerm, currentTerm.get(), votedFor, state);

        if (candidateTerm < currentTerm.get()) {
            return new RequestVoteResponse(currentTerm.get(), false, currentBrokerId);
        }

        if (candidateTerm > currentTerm.get()) {
            becomeFollower(candidateTerm, null);
        }

        boolean canVote    = (votedFor == null || votedFor.equals(candidateId));
        boolean logOk      = request.getLastLogOffset() >= getLatestLogOffset();

        if (canVote && logOk) {
            votedFor = candidateId;
            resetElectionDeadline();
            log.info("Granted vote to {} for term {}", candidateId, candidateTerm);
            return new RequestVoteResponse(currentTerm.get(), true, currentBrokerId);
        }

        log.info("Denied vote to {}. canVote={}, logUpToDate={}, votedFor={}, ourOffset={}, candidateOffset={}",
                candidateId, canVote, logOk, votedFor, getLatestLogOffset(), request.getLastLogOffset());
        return new RequestVoteResponse(currentTerm.get(), false, currentBrokerId);
    }

    // ── Heartbeat ────────────────────────────────────────────────────────────

    private void maybeSendHeartbeats() {
        if (state != RaftState.LEADER) return;
        brokerRegistry.getAllBrokers().stream()
                .filter(b -> !b.getBrokerId().equals(currentBrokerId))
                .forEach(this::sendHeartBeatTo);
    }

    private void sendHeartBeatTo(BrokerInfo peer) {
        try {
            HeartbeatRequest request = new HeartbeatRequest(
                    currentTerm.get(), currentBrokerId, "cluster", -1, getLatestLogOffset());

            HeartbeatResponse response = restTemplate.postForObject(
                    peer.baseUrl() + "/api/v1/raft/heartbeat",
                    request, HeartbeatResponse.class);

            if (response != null && response.getTerm() > currentTerm.get()) {
                log.info("Stepping down: peer {} has higher term {}", peer.getBrokerId(), response.getTerm());
                becomeFollower(response.getTerm(), null);
            }
        } catch (Exception e) {
            // FIX: catch here so one failed peer does NOT kill the heartbeat loop
            log.debug("Heartbeat to {} failed: {}", peer.getBrokerId(), e.getMessage());
        }
    }

    public synchronized HeartbeatResponse handleHeartBeat(HeartbeatRequest request) {
        int leaderTerm = request.getTerm();

        if (leaderTerm < currentTerm.get()) {
            log.warn("Rejecting heartbeat from stale leader {} (term {} < our term {})",
                    request.getLeaderId(), leaderTerm, currentTerm.get());
            return new HeartbeatResponse(currentTerm.get(), false, currentBrokerId);
        }

        if (leaderTerm > currentTerm.get()) {
            currentTerm.set(leaderTerm);
            votedFor = null;
        }

        if (state != RaftState.FOLLOWER) {
            log.info("Stepping down to FOLLOWER. Received heartbeat from leader {} (term {})",
                    request.getLeaderId(), leaderTerm);
            state = RaftState.FOLLOWER;
        }

        currentLeaderId = request.getLeaderId();
        // FIX: use resetElectionDeadline() — updates BOTH lastHeartbeatTime AND electionDeadline
        resetElectionDeadline();

        return new HeartbeatResponse(currentTerm.get(), true, currentBrokerId);
    }

    // ── State transitions ────────────────────────────────────────────────────

    /** Must be called while holding the monitor. */
    private void becomeLeader(int term) {
        state           = RaftState.LEADER;
        currentLeaderId = currentBrokerId;
        log.info("||  BECAME LEADER for term {}   ||", term);

        /**
         * FIX (Bug 2 — PartitionMetadata lost on leader change):
         *
         * When a new Raft leader is elected, PartitionMetadata (which broker owns which
         * partition) is in-memory only — it was built by the OLD leader during topic creation
         * and lives only in that process. The new leader's PartitionMetadata is empty.
         *
         * Fix: re-run assignPartitions() for every topic immediately on becoming leader.
         * This rebuilds the routing table from the consistent hash ring (which IS
         * deterministic — same brokers → same assignments every time).
         *
         * This is exactly what Kafka's controller does: on controller failover, the new
         * controller reads ZooKeeper and rebuilds all partition state from scratch.
         */
        try {
            List<String> topicNames = topicRepository.findAll()
                    .stream().map(t -> t.getName()).toList();

            for (String topicName : topicNames) {
                // findByName gives us partitionCount
                topicRepository.findByName(topicName).ifPresent(topic -> {
                    brokerRegistry.assignPartitions(topicName, topic.getPartitionCount());
                    log.info("Re-assigned {} partitions for topic '{}' after leader election",
                            topic.getPartitionCount(), topicName);
                });
            }
        } catch (Exception e) {
            log.error("Failed to re-assign partitions after leader election: {}", e.getMessage());
        }

        // Send heartbeats immediately to suppress follower elections
        maybeSendHeartbeats();
    }

    private synchronized void becomeFollower(int term, String leaderId) {
        state           = RaftState.FOLLOWER;
        currentTerm.set(term);
        votedFor        = null;
        currentLeaderId = leaderId;
        resetElectionDeadline();
        log.info("Became FOLLOWER for term {}. Leader: {}", term, leaderId);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private long getLatestLogOffset() {
        long max = 0;
        try {
            for (Map.Entry<String, BrokerInfo> entry : partitionMetadata.getAllAssignments().entrySet()) {
                String key = entry.getKey();
                int lastDash = key.lastIndexOf('-');
                if (lastDash <= 0) continue;
                try {
                    long off = logManager.getLatestOffset(
                            key.substring(0, lastDash),
                            Integer.parseInt(key.substring(lastDash + 1)));
                    max = Math.max(max, off);
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            log.debug("Error getting log offset: {}", e.getMessage());
        }
        return max;
    }

    public int getCurrentTerm()  { return currentTerm.get(); }
    public boolean isLeader()    { return state == RaftState.LEADER; }
    public void resetElectionTimer() { resetElectionDeadline(); }
}