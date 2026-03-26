/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.benchmarks.results.BackPressureAnalyser.LabelledResult;
import io.kroxylicious.benchmarks.results.BackPressureAnalyser.Report;

import static org.assertj.core.api.Assertions.assertThat;

class CheckBackPressureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final BackPressureAnalyser ANALYSER = new BackPressureAnalyser();

    private static OmbResult load(String fixture) throws IOException {
        try (InputStream is = CheckBackPressureTest.class.getResourceAsStream("/" + fixture)) {
            return MAPPER.readValue(is, OmbResult.class);
        }
    }

    private static File fixtureFile(String name) throws Exception {
        URL url = CheckBackPressureTest.class.getResource("/" + name);
        assertThat(url).as("fixture %s must exist", name).isNotNull();
        return new File(url.toURI());
    }

    @Nested
    class Analyser {

        private static OmbResult saturated;
        private static OmbResult clean;

        @BeforeAll
        static void loadFixtures() throws IOException {
            saturated = load("omb-result-saturated.json");
            clean = load("omb-result-baseline.json");
        }

        @Test
        void emptyWhenNoBackPressure() {
            var reports = ANALYSER.analyse(List.of(new LabelledResult("baseline", clean)));
            assertThat(reports).isEmpty();
        }

        @Test
        void detectsBackPressureInSaturatedResult() {
            var reports = ANALYSER.analyse(List.of(new LabelledResult("baseline", saturated)));
            assertThat(reports).singleElement().satisfies(r -> {
                assertThat(r.saturated()).isTrue();
                assertThat(r.label()).isEqualTo("baseline");
                assertThat(r.delayAvgNs()).isEqualTo(10_589_077.0);
                assertThat(r.delayP99Ns()).isEqualTo(15_605_823.0);
            });
        }

        @Test
        void computesSuggestedRateBoundsFromAchievedRate() {
            // saturated fixture: publishRate ~49445/s × 1 topic × 1 producer = 49445
            // max = round(49445 / 1000) * 1000 = 49000, min = 50% of max = 24500
            var reports = ANALYSER.analyse(List.of(new LabelledResult("baseline", saturated)));
            assertThat(reports).singleElement().satisfies(r -> {
                assertThat(r.suggestedMaxRate()).isEqualTo(49_000L);
                assertThat(r.suggestedMinRate()).isEqualTo(24_500L);
            });
        }

        @Test
        void onlySaturatedResultsAppearInReports() {
            var reports = ANALYSER.analyse(List.of(
                    new LabelledResult("baseline", clean),
                    new LabelledResult("proxy-no-filters", saturated)));
            assertThat(reports).singleElement()
                    .extracting(Report::label)
                    .isEqualTo("proxy-no-filters");
        }
    }

    @Nested
    class Cli {

        @Test
        void exitZeroWhenNoBackPressure() throws Exception {
            int exit = CheckBackPressure.execute(fixtureFile("omb-result-baseline.json").getAbsolutePath());
            assertThat(exit).as("exit code should be 0 when no back-pressure").isZero();
        }

        @Test
        void exitOneWhenBackPressureDetected() throws Exception {
            int exit = CheckBackPressure.execute(fixtureFile("omb-result-saturated.json").getAbsolutePath());
            assertThat(exit).as("exit code should be 1 when back-pressure detected").isEqualTo(1);
        }

        @Test
        void exitOneWhenAnyResultIsSaturated() throws Exception {
            int exit = CheckBackPressure.execute(
                    fixtureFile("omb-result-baseline.json").getAbsolutePath(),
                    fixtureFile("omb-result-saturated.json").getAbsolutePath());
            assertThat(exit).as("exit 1 if any result is saturated").isEqualTo(1);
        }
    }
}
