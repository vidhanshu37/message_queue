package com.mq.service;

import com.mq.consumerGroup.ConsumerGroupCoordinator;
import com.mq.model.Topic;
import com.mq.repository.ConsumerOffsetRepository;
import com.mq.repository.TopicRepository;
import com.mq.storage.LogManager;
import com.mq.storage.RetentionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;


@Slf4j
@Service
@RequiredArgsConstructor
public class MetricsService {

    private final TopicRepository topicRepository;
    private final ConsumerOffsetRepository consumerOffsetRepository;
    private final LogManager logManager;
    private final ConsumerGroupCoordinator coordinator;
    private final RetentionManager retentionManager;

    public Map<String, Object> getGroupLag(String groupId, String topicName) {
        List<Topic> topics = topicRepository.findAll();
        Optional<Topic> topic = topics.stream()
                .filter(t -> t.getName().equals(topicName))
                .findFirst();

        if (topic.isEmpty()) {
            return Map.of("error", "Topic not found: " + topicName);
        }

        int partitionCount = topic.get().getPartitionCount();
        Map<Integer, Long> lagByPartition = new LinkedHashMap<>();
        long totalLag = 0;

        for (int p = 0; p < partitionCount; p++) {
            try {
                long latest = logManager.getLatestOffset(topicName, p);
                long committed = consumerOffsetRepository
                        .findByGroupIdAndTopicNameAndPartitionIndex(groupId, topicName, p)
                        .map(co -> co.getCommittedOffset())
                        .orElse(0L);

                long lag = Math.max(0, latest - committed);
                lagByPartition.put(p, lag);
                totalLag += lag;
            } catch (Exception e) {
                lagByPartition.put(p, -1L); // -1 = partition not initialized
            }
        }

        // Determine lag status
        String status = totalLag == 0 ? "CAUGHT_UP"
                : totalLag < 100 ? "SLIGHTLY_BEHIND"
                : totalLag < 1000 ? "BEHIND"
                : "CRITICALLY_BEHIND";

        return Map.of(
                "groupId", groupId,
                "topicName", topicName,
                "totalLag", totalLag,
                "lagStatus", status,
                "lagByPartition", lagByPartition,
                "partitionCount", partitionCount
        );
    }


    // Disk usage stats for a topic — size per partition + totals.
    public Map<String, Object> getTopicStorageStats(String topicName) {
        Optional<Topic> topic = topicRepository.findAll().stream()
                .filter(t -> t.getName().equals(topicName))
                .findFirst();

        if (topic.isEmpty()) {
            return Map.of("error", "Topic not found: " + topicName);
        }

        int partitionCount = topic.get().getPartitionCount();
        Map<Integer, Map<String, Object>> partitionStats = new LinkedHashMap<>();
        long totalBytes = 0;
        int totalSegments = 0;

        for (int p = 0; p < partitionCount; p++) {
            try {
                long bytes = logManager.getPartitionSize(topicName, p);
                int segments = logManager.getSegmentCount(topicName, p);
                long offset = logManager.getLatestOffset(topicName, p);

                partitionStats.put(p, Map.of(
                        "sizeBytes", bytes,
                        "sizeMB", Math.round(bytes / 1024.0 / 1024.0 * 100) / 100.0,
                        "segmentCount", segments,
                        "latestOffset", offset
                ));

                totalBytes += bytes;
                totalSegments += segments;
            } catch (Exception e) {
                partitionStats.put(p, Map.of("error", e.getMessage()));
            }
        }

        return Map.of(
                "topicName", topicName,
                "partitionCount", partitionCount,
                "totalSizeBytes", totalBytes,
                "totalSizeMB", Math.round(totalBytes / 1024.0 / 1024.0 * 100) / 100.0,
                "totalSegments", totalSegments,
                "partitions", partitionStats
        );
    }

    public Map<String, Object> getClusterSummary() {
        List<Topic> topics = topicRepository.findAll();
        List<String> groups = coordinator.listGroups();

        long totalMessages = 0;
        long totalBytes = 0;

        List<Map<String, Object>> topicSummaries = new ArrayList<>();

        for (Topic topic : topics) {
            long topicMessages = 0;
            long topicBytes = 0;

            for (int p = 0; p < topic.getPartitionCount(); p++) {
                try {
                    topicMessages += logManager.getLatestOffset(topic.getName(), p);
                    topicBytes += logManager.getPartitionSize(topic.getName(), p);
                } catch (Exception ignored) {
                }
            }

            topicSummaries.add(Map.of(
                    "name", topic.getName(),
                    "partitionCount", topic.getPartitionCount(),
                    "totalMessages", topicMessages,
                    "totalSizeBytes", topicBytes
            ));

            totalMessages += topicMessages;
            totalBytes += topicBytes;
        }

        return Map.of(
                "topicCount", topics.size(),
                "groupCount", groups.size(),
                "groups", groups,
                "totalMessages", totalMessages,
                "totalSizeBytes", totalBytes,
                "totalSizeMB", Math.round(totalBytes / 1024.0 / 1024.0 * 100) / 100.0,
                "topics", topicSummaries,
                "retentionPolicy", Map.of(
                        "retentionHours", retentionManager.getRetentionHours(),
                        "maxBytesPerPart", retentionManager.getRetentionMaxBytes(),
                        "checkIntervalMs", retentionManager.getCheckIntervalMs()
                )
        );
    }

    public Map<String, Object> getAllGroupsLag(String topicName) {
        List<String> groups = coordinator.listGroups();
        Map<String, Object> result = new LinkedHashMap<>();

        for (String groupId : groups) {
            result.put(groupId, getGroupLag(groupId, topicName));
        }

        return Map.of(
                "topicName", topicName,
                "groups", result
        );
    }
}