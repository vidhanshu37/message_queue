package com.mq.dto.response;

import com.mq.consumerGroup.GroupState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JoinGroupResponse {
    private String groupId;
    private String consumerId;

    private int generation;

    private GroupState groupState;

    private List<Integer> assignedPartitions;

    private String topicName;
    private String message;
}
