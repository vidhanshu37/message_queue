package com.mq.controller;

import com.mq.dto.response.ConsumeResponse;
import com.mq.service.ConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/consumer")
@RequiredArgsConstructor
public class ConsumerController {
    private final ConsumerService consumerService;

    /**
     * Poll messages from partition
     * <p>
     * returns :
     * 200 OK - messages were available with nextOffset
     * 204 No content - partition empty
     */
    @GetMapping("/consume")
    public ResponseEntity<ConsumeResponse> consume(
            @RequestParam String topic,
            @RequestParam String group,
            @RequestParam(defaultValue = "0") int partition,
            @RequestParam(defaultValue = "-1") long offset) {

        ConsumeResponse response = consumerService.consume(
                topic, group, partition, offset);

        // if response empty -> return 204 "No content" empty partition
        if (response.isEmpty()) {
            HttpHeaders headers = new HttpHeaders();
            headers.add("Retry-After", "1");

            return ResponseEntity.status(HttpStatus.NO_CONTENT)
                    .headers(headers)
                    .build();
        }

        return ResponseEntity.ok(response);
    }

    /**
     * Get consumer lag for partition + group
     * lag = latestOffset - committedOffset
     */
    @GetMapping("/lag")
    public ResponseEntity<?> getLag(
            @RequestParam String topic,
            @RequestParam String group,
            @RequestParam(defaultValue = "0") int partition) {

        long latestOffset = consumerService.getLatestOffset(topic, partition);
        long committed = consumerService.getCommittedOffset(topic, group, partition);
        long lag = Math.max(0, latestOffset - committed);

        return ResponseEntity.ok(
                Map.of(
                        "topic", topic,
                        "group", group,
                        "partition", partition,
                        "latestOffset", latestOffset,
                        "committedOffset", committed,
                        "lag", lag
                )
        );
    }
}
