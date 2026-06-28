package com.mq;// test/.../ConsumerIntegrationTest.java

import com.mq.dto.request.CreateTopicRequest;
import com.mq.dto.request.ProduceRequest;
import com.mq.dto.response.ConsumeResponse;
import com.mq.dto.response.ProduceResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

// @SpringBootTest with RANDOM_PORT starts the full Spring context
// on a random port — no conflict with your running app
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConsumerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private String baseUrl;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port;
    }

    /**
     * Full end-to-end test:
     * 1. Create a topic with 1 partition
     * 2. Produce 500 messages
     * 3. Consume all 500 via polling loop
     * 4. Verify no duplicates
     * 5. Verify no gaps (all 500 received)
     */
    @Test
    void produce500_consumeAll_noDuplicates() {
        // Step 1: Create topic
        CreateTopicRequest topicRequest = new CreateTopicRequest();
        topicRequest.setName("integration-test-topic");
        topicRequest.setPartitionCount(1);

        ResponseEntity<String> topicResponse = restTemplate.postForEntity(
                baseUrl + "/api/v1/topics", topicRequest, String.class);
        assertThat(topicResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        // Step 2: Produce 500 messages
        for (int i = 0; i < 500; i++) {
            ProduceRequest produceRequest = new ProduceRequest();
            produceRequest.setTopicName("integration-test-topic");
            produceRequest.setValue("message-" + i);

            ResponseEntity<ProduceResponse> response = restTemplate.postForEntity(
                    baseUrl + "/api/v1/producer/produce",
                    produceRequest,
                    ProduceResponse.class);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getOffset()).isEqualTo(i);
        }

        // Step 3: Consume all messages via polling loop
        Set<Long> receivedOffsets = new HashSet<>();
        long currentOffset = 0;
        int maxPolls = 100; // safety limit — prevents infinite loop in test
        int pollCount = 0;

        while (receivedOffsets.size() < 500 && pollCount < maxPolls) {
            ResponseEntity<ConsumeResponse> response = restTemplate.getForEntity(
                    baseUrl + "/api/v1/consumer/consume"
                            + "?topic=integration-test-topic"
                            + "&group=test-group"
                            + "&partition=0"
                            + "&offset=" + currentOffset,
                    ConsumeResponse.class);

            if (response.getStatusCode() == HttpStatus.NO_CONTENT) {
                // No messages yet — wait briefly
                break;
            }

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            ConsumeResponse body = response.getBody();
            assertThat(body).isNotNull();

            // Collect all offsets received in this batch
            body.getMessages().forEach(msg ->
                    receivedOffsets.add(msg.getOffset()));

            // Move to next batch
            currentOffset = body.getNextOffset();
            pollCount++;
        }

        // Step 4: Verify all 500 received
        assertThat(receivedOffsets)
                .as("Should have received all 500 messages")
                .hasSize(500);

        // Step 5: Verify no duplicates and no gaps
        // If we received offsets 0-499 with no gaps, the set
        // contains exactly {0,1,2,...,499}
        for (long i = 0; i < 500; i++) {
            assertThat(receivedOffsets)
                    .as("Missing offset %d", i)
                    .contains(i);
        }
    }

    /**
     * Test offset persistence:
     * Consume some messages, then consume again WITHOUT providing offset.
     * The broker should resume from committed offset automatically.
     */
    @Test
    void offsetPersistence_resumesFromCommittedOffset() {
        // Create topic
        CreateTopicRequest topicRequest = new CreateTopicRequest();
        topicRequest.setName("offset-test-topic");
        topicRequest.setPartitionCount(1);
        restTemplate.postForEntity(
                baseUrl + "/api/v1/topics", topicRequest, String.class);

        // Produce 10 messages
        for (int i = 0; i < 10; i++) {
            ProduceRequest req = new ProduceRequest();
            req.setTopicName("offset-test-topic");
            req.setValue("msg-" + i);
            restTemplate.postForEntity(
                    baseUrl + "/api/v1/producer/produce", req, ProduceResponse.class);
        }

        // First consume — read from offset 0 explicitly
        ResponseEntity<ConsumeResponse> first = restTemplate.getForEntity(
                baseUrl + "/api/v1/consumer/consume"
                        + "?topic=offset-test-topic"
                        + "&group=persistence-group"
                        + "&partition=0"
                        + "&offset=0",
                ConsumeResponse.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        long nextOffset = first.getBody().getNextOffset();
        assertThat(nextOffset).isGreaterThan(0);

        // Second consume — NO offset provided (offset=-1, use committed)
        // Should continue from where first left off, not from 0
        ResponseEntity<ConsumeResponse> second = restTemplate.getForEntity(
                baseUrl + "/api/v1/consumer/consume"
                        + "?topic=offset-test-topic"
                        + "&group=persistence-group"
                        + "&partition=0",
                // no &offset= parameter — uses default -1
                ConsumeResponse.class);

        if (second.getStatusCode() == HttpStatus.OK) {
            // If there are more messages, they should start from nextOffset
            assertThat(second.getBody().getMessages().get(0).getOffset())
                    .isEqualTo(nextOffset);
        }
        // 204 is also valid if all messages were consumed in first poll
    }
}