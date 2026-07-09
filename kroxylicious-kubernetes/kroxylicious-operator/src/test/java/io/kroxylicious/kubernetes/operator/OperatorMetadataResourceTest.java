/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorMetadataResourceTest {

    @Test
    void shouldReturnOperatorVersionInfo() {
        var versionInfo = OperatorMain.versionInfo();

        assertThat(versionInfo.version()).isEqualTo("operator-metadata-version-test");
        assertThat(versionInfo.commitId()).isEqualTo("operator-metadata-commit-test");
    }

    @Test
    void operatorMetadataResourceShouldBePackagedInMainClasses() throws IOException {
        // Keep this path in sync with OperatorMain.OPERATOR_METADATA_RESOURCE
        Path metadata = Path.of("target/classes/META-INF/kroxylicious/operator/metadata.properties");

        assertThat(metadata).isRegularFile();

        var properties = new Properties();
        try (var reader = Files.newBufferedReader(metadata)) {
            properties.load(reader);
        }

        assertThat(properties.getProperty("kroxylicious.version")).isNotBlank();
        assertThat(properties.getProperty("git.commit.id")).isNotBlank();

        String content = Files.readString(metadata);
        assertThat(content)
                .as("metadata should not contain unresolved maven properties")
                .doesNotContain("${project.version}")
                .doesNotContain("${git.commit.id}");
    }
}
