package com.mq.raft;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class RaftElectionTest {

    /**
     * randomized timeouts are within expected range
     */
    @Test
    void electionTimeouts_areRandomAndWithinRange() {
        int min = 150;
        int max = 300;

        Random random = new Random();

        for(int i=0; i<1000; i++) {
            int timeout = min + random.nextInt(max - min);
            assertThat(timeout).isGreaterThanOrEqualTo(min).isLessThan(max);
        }
    }

    @Test
    void terms_onlyIncrement() {
        AtomicInteger term = new AtomicInteger(0);

        // simulate 100 election
        for(int i=0; i<100; i++) {
            int before = term.get();
            int after = term.incrementAndGet();
            assertThat(after).isGreaterThan(before);
        }
        assertThat(term.get()).isEqualTo(100);
    }

    // Majority calculation is correct - wrong majority = split brain possible(multiple leader)
    @Test
    void majorityCalculation_isCorrect() {
        // 3 brokers → need 2 votes
        assertThat((3 / 2) + 1).isEqualTo(2);

        // 5 brokers → need 3 votes
        assertThat((5 / 2) + 1).isEqualTo(3);

        // 1 broker → need 1 vote (self)
        assertThat((1 / 2) + 1).isEqualTo(1);
    }
}
