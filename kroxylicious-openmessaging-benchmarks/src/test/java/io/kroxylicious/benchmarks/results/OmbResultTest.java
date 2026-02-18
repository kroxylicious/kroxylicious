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
    void publishRateIsDeserialized() {
        assertThat(baseline.getPublishRate()).containsExactly(50000.0, 49800.0, 50200.0);
    }

    @Test
    void consumeRateIsDeserialized() {
        assertThat(baseline.getConsumeRate()).containsExactly(49900.0, 50100.0, 50000.0);
    }

    @Test
    void endToEndLatencyAvgIsDeserialized() {
        assertThat(baseline.getEndToEndLatencyAvg()).containsExactly(8.50, 9.20, 8.80);
    }

    @Test
    void endToEndLatency99pctIsDeserialized() {
        assertThat(baseline.getEndToEndLatency99pct()).containsExactly(35.40, 36.80, 36.10);
    }

    @Test
    void arrayAverageComputesMean() {
        assertThat(OmbResult.arrayAverage(new double[]{ 8.50, 9.20, 8.80 }))
                .isCloseTo(8.8333, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void arrayAverageReturnsZeroForNull() {
        assertThat(OmbResult.arrayAverage(null)).isEqualTo(0.0);
    }

    @Test
    void arrayAverageReturnsZeroForEmpty() {
        assertThat(OmbResult.arrayAverage(new double[0])).isEqualTo(0.0);
    }
}
