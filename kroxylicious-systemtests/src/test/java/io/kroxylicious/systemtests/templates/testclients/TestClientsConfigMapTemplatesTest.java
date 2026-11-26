/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.fabric8.kubernetes.api.model.ConfigMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestClientsConfigMapTemplatesTest {

    @Test
    void jarConfigMapHoldsJarAsBinaryDataKeyedByFileName(@TempDir Path tempDir) throws IOException {
        // Given
        byte[] content = { 0x50, 0x4b, 0x03, 0x04, 0x00, (byte) 0xff };
        Path jar = Files.write(tempDir.resolve("my.jar"), content);

        // When
        ConfigMap configMap = TestClientsConfigMapTemplates.jarConfigMap("ns", "my-jars", jar).build();

        // Then
        assertThat(configMap.getMetadata().getNamespace()).isEqualTo("ns");
        assertThat(configMap.getMetadata().getName()).isEqualTo("my-jars");
        assertThat(configMap.getData()).isNullOrEmpty();
        assertThat(configMap.getBinaryData())
                .containsOnlyKeys("my.jar")
                .extractingByKey("my.jar")
                .satisfies(encoded -> assertThat(Base64.getDecoder().decode(encoded)).isEqualTo(content));
    }

    @Test
    void jarConfigMapFailsForMissingJar(@TempDir Path tempDir) {
        // Given
        Path missing = tempDir.resolve("missing.jar");

        // When/Then
        assertThatThrownBy(() -> TestClientsConfigMapTemplates.jarConfigMap("ns", "my-jars", missing))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("missing.jar");
    }
}
