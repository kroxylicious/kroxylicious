/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

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
    void publishRateMeanIsComputed() {
        // [50000.0, 49800.0, 50200.0] -> mean = 50000.0
        assertThat(baseline.getPublishRateMean())
                .isCloseTo(50000.0, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void consumeRateMeanIsComputed() {
        // [49900.0, 50100.0, 50000.0] -> mean = 50000.0
        assertThat(baseline.getConsumeRateMean())
                .isCloseTo(50000.0, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void endToEndLatencyAvgMeanIsComputed() {
        // [8.50, 9.20, 8.80] -> mean = 8.8333
        assertThat(baseline.getEndToEndLatencyAvgMean())
                .isCloseTo(8.8333, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void endToEndLatency99pctMeanIsComputed() {
        // [35.40, 36.80, 36.10] -> mean = 36.10
        assertThat(baseline.getEndToEndLatency99pctMean())
                .isCloseTo(36.10, org.assertj.core.data.Offset.offset(0.01));
    }
}
