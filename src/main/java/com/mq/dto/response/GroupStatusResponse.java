package com.mq.dto.response;

import com.mq.consumerGroup.GroupState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Full group status — used by admin/debug endpoint.
 * Shows all members, their assignments, and group health.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupStatusResponse {
    private String groupId;
    private GroupState state;
    private int generation;
    private String topicName;
    private int totalPartitions;
    private int memberCount;

    /** consumerId → list of assigned partition indices */
    private Map<String, List<Integer>> assignments;

    /** consumerId → ms since last heartbeat */
    private Map<String, Long> heartbeatAge;

    private String message;
}