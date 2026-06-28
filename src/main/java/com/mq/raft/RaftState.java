package com.mq.raft;

/**
 * there are 3 states a broker can be at any time
 * <p>
 * every broker starts as FOLLOWER.
 * FOLLOWER -> CANDIDATE when election timeout fires
 * CANDIDATE -> LEADER when majority votes received
 * LEADER/CANDIDATE -> FOLLOWER when higher term seen
 *
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
