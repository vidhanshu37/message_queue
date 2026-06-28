package com.mq.dto.response;

import com.mq.consumerGroup.GroupState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AssignmentResponse {
    private String groupId;
    private String consumerId;
    private String topicName;

    private int generation;

    private GroupState groupState;

    private List<Integer> assignedPartitions;

    private String message;
}