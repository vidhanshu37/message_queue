package com.mq.storage;

import com.mq.repository.TopicRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetentionManager {
    private final LogManager logManager;
    private final TopicRepository topicRepository;

    // keeps msg for 7 days
    @Value("${retention.hours:300000}")
    private long retentionHours;

    // max byte 1GB
    @Value("${retention.max.bytes:300000}")
    private long retentionMaxBytes;

    // retention checks in every 5 mins
    @Value("${retention.check.interval.ms:300000}")
    private long checkIntervalMs;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "retention-manager");
                t.setDaemon(true);
                return t;
            });

    @PostConstruct
    public void start() {
        scheduler.scheduleAtFixedRate(
                this::runRetentionCheck,
                checkIntervalMs,
                checkIntervalMs,
                TimeUnit.MILLISECONDS
        );
        log.info("RetentionManager started. Policy: {}h time / {}MB size, check every {}ms",
                retentionHours, retentionMaxBytes / (1024 * 1024), checkIntervalMs);
    }

    @PostConstruct
    public void stop() {
        scheduler.shutdownNow();
    }

    // main retention
    private void runRetentionCheck() {
        log.debug("Running retention check ..");
        try {
            topicRepository.findAll().forEach(
                    topic -> {
                        for (int i = 0; i < topic.getPartitionCount(); i++) {
                            try {
                                enforceRetention(topic.getName(), i);
                            } catch (Exception e) {
                                log.warn("Retention check failed for {}-{}: {}",
                                        topic.getName(), i, e.getMessage());
                            }
                        }
                    }
            );
        } catch (Exception e) {
            log.error("Retention check loop failed: {}", e.getMessage());
        }
    }

    // enforce retention for one partition
    // runs both time-based and size-based
    private void enforceRetention(String topicName, int partitionIdx) throws IOException {
        int timeDeleted = logManager.deleteSegmentsByTime(topicName, partitionIdx, retentionHours);

        int sizeDeleted = logManager.deleteSegmentsBySize(topicName, partitionIdx, retentionHours);

        if (timeDeleted > 0 || sizeDeleted > 0) {
            log.info("Retention: {}-{} deleted {} time-expired + {} size-exceeded segments",
                    topicName, partitionIdx, timeDeleted, sizeDeleted);
        }
    }

    public void runNow() {
        log.info("Manual retention check triggered");
        scheduler.execute(this::runRetentionCheck);
    }

    public long getRetentionHours() {
        return retentionHours;
    }

    public long getRetentionMaxBytes() {
        return retentionMaxBytes;
    }

    public long getCheckIntervalMs() {
        return checkIntervalMs;
    }
}
