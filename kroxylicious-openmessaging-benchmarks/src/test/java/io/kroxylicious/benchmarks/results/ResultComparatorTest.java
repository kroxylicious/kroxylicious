/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ResultComparatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Extracts the data rows belonging to a named section from the comparison output.
     * A section starts with a header line containing the section name and ends
     * at the next blank line or end of output. Data rows are the lines after the
     * column header and separator lines.
     */
    static List<String> extractSection(String output, String sectionName) {
        List<String> rows = new ArrayList<>();
        boolean inSection = false;
        int headerLinesSeen = 0;

        for (String line : output.split("\n")) {
            if (line.contains(sectionName)) {
                inSection = true;
                headerLinesSeen = 0;
                continue;
            }
            if (inSection) {
                if (line.isBlank()) {
                    break;
                }
                headerLinesSeen++;
                if (headerLinesSeen > 2) {
                    rows.add(line);
                }
            }
        }
        return rows;
    }

    /**
     * Parsed column values from a comparison row.
     */
    record RowValues(String baseline, String candidate, String delta) {}

    /**
     * Finds the data row for a given metric label within a section's rows and
     * parses it into positional column values.
     */
    static RowValues findRow(List<String> sectionRows, String metricLabel) {
        return sectionRows.stream()
                .filter(row -> row.trim().startsWith(metricLabel + " "))
                .findFirst()
                .map(row -> {
                    String[] values = row.trim().substring(metricLabel.length()).trim().split("\\s+");
                    return new RowValues(values[0], values[1], values[2]);
                })
                .orElse(null);
    }

    private static String runComparison() throws IOException {
        OmbResult baseline;
        OmbResult candidate;
        try (InputStream baseIs = ResultComparatorTest.class.getResourceAsStream("/omb-result-baseline.json");
                InputStream proxyIs = ResultComparatorTest.class.getResourceAsStream("/omb-result-proxy.json")) {
            baseline = MAPPER.readValue(baseIs, OmbResult.class);
            candidate = MAPPER.readValue(proxyIs, OmbResult.class);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            new ResultComparator(baseline, candidate).compare(ps);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    @Nested
    class PublishLatency {

        private static List<String> publishLatencyRows;

        @BeforeAll
        static void setUp() throws IOException {
            String output = runComparison();
            publishLatencyRows = extractSection(output, "Publish Latency");
        }

        @Test
        void sectionContainsRows() {
            assertThat(publishLatencyRows)
                    .as("Publish Latency section should contain data rows")
                    .isNotEmpty();
        }

        @Test
        void avgShowsCorrectValues() {
            RowValues row = findRow(publishLatencyRows, "Avg");
            assertThat(row).as("Publish Latency should have an Avg row").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("5.12");
            assertThat(row.candidate()).as("candidate").isEqualTo("6.34");
        }

        @Test
        void p99ShowsCorrectValues() {
            RowValues row = findRow(publishLatencyRows, "p99");
            assertThat(row).as("Publish Latency should have a p99 row (not p99.9)").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("25.60");
            assertThat(row.candidate()).as("candidate").isEqualTo("29.10");
        }

        @Test
        void p999ShowsCorrectValues() {
            RowValues row = findRow(publishLatencyRows, "p99.9");
            assertThat(row).as("Publish Latency should have a p99.9 row").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("48.30");
            assertThat(row.candidate()).as("candidate").isEqualTo("55.70");
        }
    }

    @Nested
    class EndToEndLatency {

        private static List<String> endToEndLatencyRows;

        @BeforeAll
        static void setUp() throws IOException {
            String output = runComparison();
            endToEndLatencyRows = extractSection(output, "End-to-End Latency");
        }

        @Test
        void sectionContainsRows() {
            assertThat(endToEndLatencyRows)
                    .as("End-to-End Latency section should contain data rows")
                    .isNotEmpty();
        }

        @Test
        void avgShowsCorrectValues() {
            // Uses aggregatedEndToEndLatencyAvg (OMB full-run scalar)
            RowValues row = findRow(endToEndLatencyRows, "Avg");
            assertThat(row).as("End-to-End Latency should have an Avg row").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("8.95");
            assertThat(row.candidate()).as("candidate").isEqualTo("10.70");
        }

        @Test
        void p99ShowsCorrectValues() {
            // Uses aggregatedEndToEndLatency99pct (OMB full-run scalar, not mean of per-window p99s)
            RowValues row = findRow(endToEndLatencyRows, "p99");
            assertThat(row).as("End-to-End Latency should have a p99 row (not p99.9)").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("38.50");
            assertThat(row.candidate()).as("candidate").isEqualTo("45.20");
        }
    }

    @Nested
    class Significance {

        @Test
        void latencySectionHeaderContainsMwuPColumn() throws IOException {
            assertThat(runComparison()).contains("MWU p");
        }

        @Test
        void publishLatencyDataRowsShowPValue() throws IOException {
            List<String> dataRows = extractSection(runComparison(), "Publish Latency");
            assertThat(dataRows).allSatisfy(row -> assertThat(row).matches(".*\\d+\\.\\d{4}.*"));
        }

        @Test
        void endToEndLatencyDataRowsShowPValue() throws IOException {
            List<String> dataRows = extractSection(runComparison(), "End-to-End Latency");
            assertThat(dataRows).allSatisfy(row -> assertThat(row).matches(".*\\d+\\.\\d{4}.*"));
        }

        @Test
        void footnoteExplainsSignificanceMarker() throws IOException {
            assertThat(runComparison()).contains("p < 0.05");
        }

        @Test
        void throughputDataRowsShowPValue() throws IOException {
            List<String> dataRows = extractSection(runComparison(), "Total Throughput");
            assertThat(dataRows).allSatisfy(row -> assertThat(row).matches(".*\\d+\\.\\d{4}.*"));
        }
    }

    @Nested
    class Throughput {

        private static List<String> throughputRows;

        @BeforeAll
        static void setUp() throws IOException {
            String output = runComparison();
            throughputRows = extractSection(output, "Total Throughput");
        }

        @Test
        void sectionContainsRows() {
            assertThat(throughputRows)
                    .as("Throughput section should contain data rows")
                    .isNotEmpty();
        }

        @Test
        void publishRateShowsTotalAcrossAllProducers() {
            // Baseline: [5000, 4980, 5020] mean=5000 * 10 topics * 1 producer = 50000
            // Proxy: [4950, 4920, 4970] mean=4946.67 * 10 * 1 = 49466.67
            RowValues row = findRow(throughputRows, "Publish Rate");
            assertThat(row).as("Total Throughput should have a Publish Rate row").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("50000.00");
            assertThat(row.candidate()).as("candidate").isEqualTo("49466.67");
        }

        @Test
        void consumeRateShowsTotalAcrossAllConsumers() {
            // Baseline: [4990, 5010, 5000] mean=5000 * 10 topics * 1 consumer = 50000
            // Proxy: [4940, 4960, 4950] mean=4950 * 10 * 1 = 49500
            RowValues row = findRow(throughputRows, "Consume Rate");
            assertThat(row).as("Total Throughput should have a Consume Rate row").isNotNull();
            assertThat(row.baseline()).as("baseline").isEqualTo("50000.00");
            assertThat(row.candidate()).as("candidate").isEqualTo("49500.00");
        }
    }
}
