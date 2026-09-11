/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

class OmbResultTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static OmbResult baseline;

    @BeforeAll
    static void loadFixture() throws IOException {
        try (InputStream is = OmbResultTest.class.getResourceAsStream("/omb-result-baseline.json")) {
            baseline = MAPPER.readValue(is, OmbResult.class);
        }
    }

    @Test
    void publishLatencyAvgIsDeserialized() {
        assertThat(baseline.getPublishLatencyAvg()).isEqualTo(5.12);
    }

    @Test
    void publishLatency50pctIsDeserialized() {
        assertThat(baseline.getPublishLatency50pct()).isEqualTo(3.85);
    }

    @Test
    void publishLatency95pctIsDeserialized() {
        assertThat(baseline.getPublishLatency95pct()).isEqualTo(12.40);
    }

    @Test
    void publishLatency99pctIsDeserialized() {
        assertThat(baseline.getPublishLatency99pct()).isEqualTo(25.60);
    }

    @Test
    void publishLatency999pctIsDeserialized() {
        assertThat(baseline.getPublishLatency999pct()).isEqualTo(48.30);
    }

    @Test
    void publishRateIsTotalAcrossAllProducers() {
        // per-producer: [5000, 4980, 5020] -> mean 5000; topics=10, producersPerTopic=1
        // total = 5000 * 10 * 1 = 50000
        assertThat(baseline.getPublishRate()).isCloseTo(50000.0, withPercentage(0.01));
    }

    @Test
    void consumeRateIsTotalAcrossAllConsumers() {
        // per-consumer: [4990, 5010, 5000] -> mean 5000; topics=10, consumersPerTopic=1
        // total = 5000 * 10 * 1 = 50000
        assertThat(baseline.getConsumeRate()).isCloseTo(50000.0, withPercentage(0.01));
    }

    @Test
    void aggregatedEndToEndLatency99pctIsDeserialized() {
        assertThat(baseline.getAggregatedEndToEndLatency99pct()).isEqualTo(38.50);
    }

    @Test
    void aggregatedEndToEndLatency50pctIsDeserialized() {
        assertThat(baseline.getAggregatedEndToEndLatency50pct()).isEqualTo(6.40);
    }

    @Test
    void aggregatedEndToEndLatency999pctIsDeserialized() {
        assertThat(baseline.getAggregatedEndToEndLatency999pct()).isEqualTo(69.80);
    }

    @Test
    void publishDelayLatencyAvgIsDeserialized() {
        assertThat(baseline.getPublishDelayLatencyAvgNs()).isEqualTo(74.0);
    }

    @Test
    void publishDelayLatency99pctIsDeserialized() {
        assertThat(baseline.getPublishDelayLatency99pctNs()).isEqualTo(182.0);
    }

    @Test
    void publishLatencyAvgWindowsAreDeserialized() {
        assertThat(baseline.getPublishLatencyAvgWindows()).containsExactly(4.8, 5.1, 5.5);
    }

    @Test
    void publishLatency50pctWindowsAreDeserialized() {
        assertThat(baseline.getPublishLatency50pctWindows()).containsExactly(3.5, 3.9, 4.1);
    }

    @Test
    void publishLatency95pctWindowsAreDeserialized() {
        assertThat(baseline.getPublishLatency95pctWindows()).containsExactly(11.0, 12.5, 13.2);
    }

    @Test
    void publishLatency99pctWindowsAreDeserialized() {
        assertThat(baseline.getPublishLatency99pctWindows()).containsExactly(24.0, 25.0, 26.0);
    }

    @Test
    void publishLatency999pctWindowsAreDeserialized() {
        assertThat(baseline.getPublishLatency999pctWindows()).containsExactly(45.0, 48.0, 51.0);
    }

    @Test
    void endToEndLatencyAvgWindowsAreDeserialized() {
        assertThat(baseline.getEndToEndLatencyAvgWindows()).containsExactly(8.2, 9.0, 9.5);
    }

    @Test
    void endToEndLatency50pctWindowsAreDeserialized() {
        assertThat(baseline.getEndToEndLatency50pctWindows()).containsExactly(6.0, 6.4, 6.8);
    }

    @Test
    void endToEndLatency95pctWindowsAreDeserialized() {
        assertThat(baseline.getEndToEndLatency95pctWindows()).containsExactly(18.0, 19.0, 20.5);
    }

    @Test
    void endToEndLatency99pctWindowsAreDeserialized() {
        assertThat(baseline.getEndToEndLatency99pctWindows()).containsExactly(37.0, 38.5, 39.0);
    }

    @Test
    void endToEndLatency999pctWindowsAreDeserialized() {
        assertThat(baseline.getEndToEndLatency999pctWindows()).containsExactly(65.0, 70.0, 74.0);
    }

    @Nested
    class NullRateArrays {

        private static OmbResult nullRates;

        @BeforeAll
        static void loadFixture() throws IOException {
            // Minimal JSON with no publishRate/consumeRate fields (absent → null after deserialization)
            nullRates = MAPPER.readValue(
                    "{\"topics\":1,\"producersPerTopic\":1,\"consumersPerTopic\":1}",
                    OmbResult.class);
        }

        @Test
        void getPublishRateReturnsZeroWhenArrayIsNull() {
            assertThat(nullRates.getPublishRate()).isZero();
        }

        @Test
        void getConsumeRateReturnsZeroWhenArrayIsNull() {
            assertThat(nullRates.getConsumeRate()).isZero();
        }

        @Test
        void getPublishRateWindowsReturnsEmptyWhenArrayIsNull() {
            assertThat(nullRates.getPublishRateWindows()).isEmpty();
        }

        @Test
        void getConsumeRateWindowsReturnsEmptyWhenArrayIsNull() {
            assertThat(nullRates.getConsumeRateWindows()).isEmpty();
        }
    }
}
