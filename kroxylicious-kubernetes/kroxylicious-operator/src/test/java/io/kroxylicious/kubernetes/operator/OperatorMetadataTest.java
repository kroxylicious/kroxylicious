/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.VersionInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the operator ships and reports its own build metadata, rather than falling back to
 * the {@code kroxylicious-runtime} {@code META-INF/metadata.properties} that happens to be on the
 * classpath.
 */
class OperatorMetadataTest {

    @Test
    void reportsOperatorsOwnBuildMetadata() {
        // Given the operator's build produced its own metadata resource (see OperatorMain.METADATA_RESOURCE)

        // When the operator loads its version information
        var versionInfo = VersionInfo.fromResource(OperatorMain.METADATA_RESOURCE);

        // Then the version and commit id come from the operator's own resource, not unknown fallbacks
        assertThat(versionInfo.version())
                .as("operator version should be populated from its own metadata resource")
                .isNotBlank()
                .isNotEqualTo("unknown");
        assertThat(versionInfo.commitId())
                .as("operator commit id should be populated from its own metadata resource")
                .isNotBlank()
                .isNotEqualTo("unknown");
    }
}
