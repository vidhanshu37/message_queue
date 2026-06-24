package com.mq.storage;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class LogManager {
    // Base directory: e.g. /tmp/mq-data/
    private final Path dataDir;

    // Key: "topicName-partitionIndex", value: list of segments (oldest first
    private final Map<String, List<LogSegment>> partitionSegments = new ConcurrentHashMap<>();

    public LogManager(@Value("${mq.data.dir:/tmp/mq-data}") String dataDir) throws IOException {
        this.dataDir = Path.of(dataDir);
        Files.createDirectories(this.dataDir);
        log.info("LogManager initialized at {}", this.dataDir);
    }

    /**
     * Called when a topic/partition is created.
     * Creates the directory and initial segment for the partition.
     */
    public void initPartition(String topicName, int partitionIndex) throws IOException {
        String key = partitionKey(topicName, partitionIndex);
        if(partitionSegments.containsKey(key)) {
            return; // already initialized
        }

        synchronized (this) {
            Path partitionDir = partitionDir(topicName, partitionIndex);
            Files.createDirectories(partitionDir);

            List<LogSegment> segments = new ArrayList<>();
            segments.add(createSegment(partitionDir, 0L));
            partitionSegments.put(key, segments);

            log.info("Initialized partition {} with initial segment", key);
        }
    }

    /**
     * Append a message to a partition.
     * Rolls to a new segment if current segment is full.
     */
    public long append(String topicName, int partitionIndex, byte[] payload) throws IOException {
        List<LogSegment> segments = getSegments(topicName, partitionIndex);
        LogSegment active = activeSegment(segments);

        if(active.isFull()) {
            log.info("Rolling segment for {}-{}", topicName, partitionIndex);
            active = rollSegment(topicName, partitionIndex, segments);
        }

        return active.append(payload);
    }

    /**
     * Write message at specific offset
     */
    public void appendAtOffset(String topicName, int partitionIdx, byte[] payload, long offset) throws IOException {
        String key = partitionKey(topicName, partitionIdx);
        List<LogSegment> segments = partitionSegments.get(key);

        if(segments == null) {
            throw new IllegalArgumentException("Partition not initialized: " + key);
        }

        LogSegment active = activeSegment(segments);
        if(active.isFull()) {
            active = rollSegment(topicName, partitionIdx, segments);
        }

        // Write at the specific offset
        active.appendAtOffset(payload, offset);
    }

    /**
     * Read messages from a partition starting at a given offset.
     */
    public List<MessageRecord> read(String topicName, int partitionIndex, long startOffset, int maxBytes) throws IOException {
        List<LogSegment> segments = getSegments(topicName, partitionIndex);

        // Find right segment: the last one whose baseOffset <= startOffset
        LogSegment targetSegment = segments.get(0);
        for(LogSegment segment : segments) {
            if(segment.getBaseOffset() <= startOffset) {
                targetSegment = segment;
            }
        }

        return targetSegment.read(startOffset, maxBytes);
    }

    /**
     * latest written offset for partition
     * so, consumers will know how far behind they are
     */
    public long getLatestOffset(String topicName, int partitionIndex) throws IOException {
        // Returns nextOffset — matches committedOffset convention (both point to next slot)
        // lag = latestOffset - committedOffset = unconsumed message count
        List<LogSegment> segments = getSegments(topicName, partitionIndex);
        return activeSegment(segments).getNextOffset();
    }

    ///  Helper function
    private LogSegment rollSegment(String topicName, int partitionIndex, List<LogSegment> segments) throws IOException {
        long newBaseOffset = activeSegment(segments).getNextOffset();
        Path partitionDir = partitionDir(topicName, partitionIndex);
        LogSegment newSegment = createSegment(partitionDir, newBaseOffset);
        segments.add(newSegment);
        return newSegment;
    }

    private LogSegment createSegment(Path dir, long baseOffset) throws IOException {
        // Filename: 20-digit zero-padded offset - 00000000000000000000.log
        String name = String.format("%020d", baseOffset);

        Path logPath = dir.resolve(name + ".log");
        Path indexPath = dir.resolve(name + ".index");

        return new LogSegment(logPath, indexPath, baseOffset);
    }

    private List<LogSegment> getSegments(String topicName, int partitionIndex) {
        String key = partitionKey(topicName, partitionIndex);
        List<LogSegment> segments = partitionSegments.get(key);
        if(segments == null) {
            throw new IllegalArgumentException("Partition not initialized: " + key);
        }
        return segments;
    }

    private LogSegment activeSegment(List<LogSegment> segments) {
        return segments.get(segments.size() - 1);
    }

    private Path partitionDir(String topicName, int partitionIndex) {
        return dataDir.resolve(topicName).resolve("partition-"+partitionIndex);
    }

    private String partitionKey(String topicName, int partitionIndex) {
        return topicName + "-" + partitionIndex;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down LogManager, closing all segments");
        partitionSegments.values().forEach(segments -> segments.forEach(seg -> {
            try {
                seg.close();
            } catch (IOException e) {
                log.error("Error closing segment {}: {}", seg.getBaseOffset(), e.getMessage());
            }
        }));
    }
}