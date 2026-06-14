package com.mq.raft;

import com.mq.cluster.BrokerRegistry;
import com.mq.cluster.PartitionMetadata;
import com.mq.dto.request.HeartbeatRequest;
import com.mq.dto.request.RequestVoteRequest;
import com.mq.dto.response.HeartbeatResponse;
import com.mq.dto.response.RequestVoteResponse;
import com.mq.model.BrokerInfo;
import com.mq.storage.LogManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.annotations.Collate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class RaftNode {

    // in production raft written on disk but for now use in-memory

    /**
     * Current term — the logical clock.
     * Incremented on every election start.
     */
    private final AtomicInteger currentTerm = new AtomicInteger(0); // thread-safe

    /**
     * who this node voted for in current term
     * volatile - Whenever a thread reads this variable, always read the latest value from main memory. Don't use a cached copy
     */
    private volatile String votedFor = null;

    // RAFT volatile state bcz - read by multiple threads and written by election logic
    @Getter
    private volatile RaftState state = RaftState.FOLLOWER;

    // leader id -> if null no leader yet
    @Getter
    private volatile String currentLeaderId = null;

    // last time received heartbeat from leader
    private volatile long lastHeartbeatTime = System.currentTimeMillis();


    /// ELECTION TIMEOUT CONFIG

    /**
     * If no heartbeat received within a random value
     * between MIN and MAX, start an election.
     * <p>
     * 150ms min -> must be > heartbeat interval(150 ms) to avoid false elections during operation
     * <p>
     * and choose random time - prevents all followers timing out simultaneously that's why we use random timeout
     */
    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;

    private static final int HEARTBEAT_INTERVAL_MS = 50; // leader sends heartbeat every 50ms

    ///  DEPENDENCY
    private final BrokerRegistry brokerRegistry;
    private final PartitionMetadata partitionMetadata;
    private final LogManager logManager;
    private final RestTemplate restTemplate;
    private final Random random = new Random();

    @Value("${broker.id:broker-1}")
    private String currentBrokerId;

    ///  BACKGROUND THREADS
    // use single thread for election

    private final ScheduledExecutorService electionExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-election");
                t.setDaemon(true);
                return t;
            });

    /**
     * leader send heartbeats independently of the election time out checker - create separate executor
     */
    private final ScheduledExecutorService heartbeatExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-heartbeat");
                t.setDaemon(true);
                return t;
            });

    public RaftNode(BrokerRegistry brokerRegistry,
                    PartitionMetadata partitionMetadata,
                    LogManager logManager,
                    RestTemplate restTemplate) {
        this.brokerRegistry = brokerRegistry;
        this.partitionMetadata = partitionMetadata;
        this.logManager = logManager;
        this.restTemplate = restTemplate;
    }

    ///  LIFECYCLE

    /**
     * Start background threads for election and heartbeat after spring context  initialized
     */
    @PostConstruct
    public void start() {
        log.info("Starting RaftNode with brokerId {} in state {}", currentBrokerId, state);

        // start election timeout checker
        // runs every 10ms to check if timeout has elapsed

        int startupDelayMs = 2000;

        electionExecutor.scheduleAtFixedRate(
                this::checkElectionTimeout,
                startupDelayMs, //getRandomElectionTimeout(),
                10, //  check every 10 ms
                java.util.concurrent.TimeUnit.MILLISECONDS
        );

        // Start heartbeat sender
        // Only actually sends if this node is LEADER
        heartbeatExecutor.scheduleAtFixedRate(
                this::maybeSendHeartbeats,
                startupDelayMs + HEARTBEAT_INTERVAL_MS,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        lastHeartbeatTime = System.currentTimeMillis() + startupDelayMs;

        log.info("RaftNode started. Initial state: {}, initial term: {}, initial leader: {}",
                state, currentTerm.get(), currentLeaderId);
    }

    @PreDestroy
    public void stop() {
        electionExecutor.shutdownNow();
        heartbeatExecutor.shutdownNow();
        log.info("RaftNode stopped");
    }

    ///  ELECTION timeout logic

    /**
     * called every 10ms by the election timer
     * check if gone too long without heartbeat
     * <p>
     * only FOLLOWER and CANDIDATE check for time out
     */
    private void checkElectionTimeout() {
        // LEADER don't need to check for timeout
        if (state == RaftState.LEADER) {
            return;
        }

        long elapsed = System.currentTimeMillis() - lastHeartbeatTime;
        long timeout = getRandomElectionTimeout();

        if (elapsed > timeout) {
            log.info("Election timeout! No heartbeat for {}ms. " +
                            "Starting election. Term: {}",
                    elapsed, currentTerm.get());
            startElection();
        }

    }

    /**
     * Get random election timeout btw MIN and MAX
     *
     */
    private long getRandomElectionTimeout() {
        return ELECTION_TIMEOUT_MIN_MS + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
    }

    ///  ELECTION logic

    /**
     * Start new election
     * <p>
     * 1. become a candidate
     * 2. increment term
     * 3. vote for self
     * 4. send request-vote
     * 5. if majority votes -> be leader
     * 6. if see higher term -> step down to follower
     * 7. if no majority then wait for next timeout
     */

    private synchronized void startElection() {
        state = RaftState.CANDIDATE;
        int newTerm = currentTerm.incrementAndGet(); // increment term atomically
        votedFor = currentBrokerId; // vote for self
        currentLeaderId = null;

        log.info("Started election for term {}. Voted for self. State: {}", newTerm, state);

        Collection<BrokerInfo> peers = brokerRegistry.getAllBrokers()
                .stream()
                .filter(b -> !b.getBrokerId().equals(currentBrokerId)) // exclude self
                .toList();

        if(peers.isEmpty()) {
            // Single node - immediately win
            log.info("No peers found — single node, becoming leader");
            becomeLeader(newTerm);
            return;
        }

        // count votes
        AtomicInteger voteCount = new AtomicInteger(1); // start with 1 vote for self
        int majority = (brokerRegistry.getBrokerCount() / 2) + 1;

        // send request vote to all peers
        List<CompletableFuture<Void>> futures = peers.stream()
                .map(peer -> requestVoteFrom(peer, newTerm, voteCount, majority))
                .toList();

        // wait for all votes with timeout
        CompletableFuture<Void> allVotes = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allVotes.get(ELECTION_TIMEOUT_MAX_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Vote collection timed out for term {}", newTerm);
        } catch (Exception e) {
            log.warn("Vote collection error: {}", e.getMessage());
        }

        // check final result
        if(state == RaftState.CANDIDATE && voteCount.get() >= majority) {
            becomeLeader(newTerm);
        } else {
            log.info("Election failed for term {}. Got {}/{} votes. " +
                            "Reverting to follower.",
                    newTerm, voteCount.get(), majority);
            state = RaftState.FOLLOWER;
        }
    }

    // send requestVote
    private CompletableFuture<Void> requestVoteFrom(BrokerInfo peer, int term, AtomicInteger voteCount, int majority) {
        return CompletableFuture.runAsync(() -> {
            try {
               // get latest log offset - ensure up-to-date
                long lastLogOffset = getLatestLogOffset();

                RequestVoteRequest request = new RequestVoteRequest(
                        term,
                        currentBrokerId,
                        lastLogOffset,
                        "cluster", // global election
                        -1
                );

                String url = peer.baseUrl() + "/api/v1/raft/vote";

                RequestVoteResponse response = restTemplate.postForObject(url, request, RequestVoteResponse.class);

                if(response == null) return;

                // if peer has higher term: revert to follower
                if(response.getTerm() > currentTerm.get()) {
                    log.info("Peer {} has higher term {}. " +
                                    "Reverting to follower.",
                            peer.getBrokerId(), response.getTerm());
                    becomeFollower(response.getTerm(), null);
                    return;
                }

                // count the vote
                if(response.isVoteGranted()) {
                    int votes = voteCount.incrementAndGet();
                    log.info("Received vote from {} for term {}. Total votes: {}/{}",
                            peer.getBrokerId(), term, votes, majority);
                }

            } catch (Exception e) {
                log.warn("Error requesting vote from {}: {}", peer.getBrokerId(), e.getMessage());
            }
        });
    }

    /// Vote granting logic - receiver side

    /**
     * Grant vote - if all conditions are true
     * 1. candidate's term >= our term
     * 2. we haven't voted for someone else for this term
     * 3. candidate's log is at least as up-to-date as ours
     *
     * and ensure elected leader has all committed data
     */
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        int candidateTerm = request.getTerm();
        String candidateId = request.getCandidateId();

        log.info("Received RequestVote from {} for term {}. Current term: {}, votedFor: {}, state: {}",
                candidateId, candidateTerm, currentTerm.get(), votedFor, state);

        // Rule 1 : if candidate's term < current term, reject
        if(candidateTerm < currentTerm.get()) {
            log.info("Rejecting vote for {} because candidate term {} is less than current term {}",
                    candidateId, candidateTerm, currentTerm.get());
            return new RequestVoteResponse(currentTerm.get(), false, currentBrokerId);
        }

        //If candidate has higher term: update our term, revert to follower, clear our vote
        if(candidateTerm > currentTerm.get()) {
            becomeFollower(candidateTerm, null);
        }

        // Rule 2: if we already voted for someone else in this term, reject
        boolean canVote = (votedFor == null || votedFor.equals(candidateId));

        // RUle 3: is candidate's log at least as up-to-date
        long ourLatestOffset = getLatestLogOffset();
        boolean logUptoDate = request.getLastLogOffset() >= ourLatestOffset;

        if(canVote && logUptoDate) {
            // Grant vote
            votedFor = candidateId;
            // Reset election timer — we just heard from a valid candidate
            lastHeartbeatTime = System.currentTimeMillis();

            log.info("Granted vote to {} for term {}",
                    candidateId, candidateTerm);
            return new RequestVoteResponse(
                    currentTerm.get(), true, currentBrokerId);
        }

        log.info("Denied vote to {}. canVote={}, logUpToDate={}, " +
                        "votedFor={}, ourOffset={}, candidateOffset={}",
                candidateId, canVote, logUptoDate,
                votedFor, ourLatestOffset,
                request.getLastLogOffset());

        return new RequestVoteResponse(
                currentTerm.get(), false, currentBrokerId);
    }

    ///  Heartbeat logic

    /**
     * Send heartbeat to all followers
     *
     * 2 purpose
     *  - leader is alive don't do election
     *  - tell follower who is current leader
     */
    private void maybeSendHeartbeats() {
        if (state != RaftState.LEADER) {
            return;
        }
        brokerRegistry.getAllBrokers().stream()
                .filter(b -> !b.getBrokerId().equals(currentBrokerId)) // exclude self
                .forEach(this::sendHeartBeatTo);
    }

    private void sendHeartBeatTo(BrokerInfo peer) {
        try {
            HeartbeatRequest request = new HeartbeatRequest(
                    currentTerm.get(),
                    currentBrokerId,
                    "cluster", // global heartbeat
                    -1,
                    getLatestLogOffset()
            );

            String url = peer.baseUrl() + "/api/v1/raft/heartbeat";

            HeartbeatResponse response = restTemplate.postForObject(url, request, HeartbeatResponse.class);

            if(response != null && response.getTerm() > currentTerm.get()) {
                // Peer has higher term — we're a stale leader
                // Step down immediately
                log.info("Stepping down: peer {} has higher term {}",
                        peer.getBrokerId(), response.getTerm());
                becomeFollower(response.getTerm(), null);
            }
        } catch (Exception e) {
            log.debug("Heartbeat to {} failed: {}",
                    peer.getBrokerId(), e.getMessage());
        }
    }

    /**
     * handle incoming heartbeat from leader
     *
     * heartbeat reset our election timer
     */
    public synchronized HeartbeatResponse handleHeartBeat(HeartbeatRequest request) {
        int leaderTerm = request.getTerm();

        // reject heartbeat from stale leader
        if(leaderTerm < currentTerm.get()) {
            log.warn("Rejecting heartbeat from stale leader {} " +
                            "(term {} < our term {})",
                    request.getLeaderId(),
                    leaderTerm, currentTerm.get());
            return new HeartbeatResponse(
                    currentTerm.get(), false, currentBrokerId);
        }

        // valid heartbeat
        if(leaderTerm > currentTerm.get()) {
            currentTerm.set(leaderTerm);
            votedFor = null;
        }

        // stepping down if we were a candidate or leader
        if(state != RaftState.FOLLOWER) {
            log.info("Stepping down to FOLLOWER. " +
                            "Received heartbeat from leader {} (term {})",
                    request.getLeaderId(), leaderTerm);
            state = RaftState.FOLLOWER;
        }

        // reset election timer
        lastHeartbeatTime = System.currentTimeMillis();
        currentLeaderId = request.getLeaderId();

        // Update partition metadata with current leader
        // So producers know where to send messages
        if (request.getPartitionIdx() >= 0) {
            brokerRegistry.getAllBrokers().stream()
                    .filter(b -> b.getBrokerId()
                            .equals(request.getLeaderId()))
                    .findFirst()
                    .ifPresent(leader ->
                            partitionMetadata.assignLeader(
                                    request.getTopicName(),
                                    request.getPartitionIdx(),
                                    leader));
        }

        return new HeartbeatResponse(
                currentTerm.get(), true, currentBrokerId);
    }

    ///  STATE TRANSITION HELPERS

    /**
     * transition to leader state
     *
     * 1. update local state
     * 2. update partition metadata so producers route here
     * 3. start sending heartbeats immediately
     */
    private synchronized void becomeLeader(int term) {
        state = RaftState.LEADER;
        currentLeaderId = currentBrokerId;
        log.info("||  BECAME LEADER for term {}   ||", term);

        // Update ALL partition assignments to point to this broker
        // In a full Raft implementation, you'd only take over
        // partitions you were previously assigned.
        // For simplicity: new leader takes all partitions.
        BrokerInfo selfInfo = brokerRegistry.getAllBrokers()
                .stream()
                .filter(b -> b.getBrokerId().equals(currentBrokerId))
                .findFirst()
                .orElse(null);

        if (selfInfo != null) {
            partitionMetadata.getAllAssignments()
                    .keySet()
                    .forEach(partitionKey -> {
                        // Parse "topicName-partitionIndex"
                        int lastDash = partitionKey.lastIndexOf('-');
                        if (lastDash > 0) {
                            String topic = partitionKey
                                    .substring(0, lastDash);
                            int partition = Integer.parseInt(
                                    partitionKey.substring(lastDash + 1));
                            partitionMetadata.assignLeader(
                                    topic, partition, selfInfo);
                        }
                    });
        }

        // Send immediate heartbeat to suppress other elections
        maybeSendHeartbeats();
    }

    /**
     * Transition to FOLLOWER state.
     * Called when we see a higher term or lose an election.
     */
    private synchronized void becomeFollower(int term, String leaderId) {
        state = RaftState.FOLLOWER;
        currentTerm.set(term);
        votedFor = null;
        currentLeaderId = leaderId;
        lastHeartbeatTime = System.currentTimeMillis();

        log.info("Became FOLLOWER for term {}. Leader: {}", term, leaderId);
    }

    /// HELPER FUNCTION
    private long getLatestLogOffset() {
        long maxOffset = 0;
        try {
            for (Map.Entry<String, BrokerInfo> entry :
                    partitionMetadata.getAllAssignments().entrySet()) {

                String partitionKey = entry.getKey();
                int lastDash = partitionKey.lastIndexOf('-');
                if (lastDash <= 0) continue;

                String topic = partitionKey.substring(0, lastDash);
                int partition = Integer.parseInt(
                        partitionKey.substring(lastDash + 1));

                try {
                    long offset = logManager
                            .getLatestOffset(topic, partition);
                    maxOffset = Math.max(maxOffset, offset);
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            log.debug("Error getting log offset: {}", e.getMessage());
        }
        return maxOffset;
    }

    public int getCurrentTerm() {
        return currentTerm.get();
    }

    public boolean isLeader() {
        return state == RaftState.LEADER;
    }

    /**
     * Called when this broker receives a valid heartbeat.
     * Resets the election timer.
     * Public so RaftController can call it.
     */
    public void resetElectionTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }
}
