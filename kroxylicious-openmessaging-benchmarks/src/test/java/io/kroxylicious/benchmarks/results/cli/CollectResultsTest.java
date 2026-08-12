/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results.cli;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class CollectResultsTest {

    @Test
    void exitOneWhenNoOptionsGiven() {
        // When
        int exit = CollectResults.execute();

        // Then
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void generatesRunMetadata(@TempDir Path outputDir) {
        // When
        int exit = CollectResults.execute("--generate-run-metadata", outputDir.toString(),
                "--scenario", "baseline", "--workload", "1topic-1kb");

        // Then
        assertThat(exit).isZero();
        assertThat(outputDir.resolve("run-metadata.json")).exists();
    }
}
