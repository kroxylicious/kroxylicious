/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;

import static org.assertj.core.api.Assertions.assertThat;

class AllInOneYamlProviderTest {

    private static final String TEST_MANIFEST = """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: test-config
              namespace: test-ns
            data:
              key: value
            ---
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: test-deployment
              namespace: test-ns
            spec:
              replicas: 1
              selector:
                matchLabels:
                  app: test
              template:
                metadata:
                  labels:
                    app: test
                spec:
                  containers:
                  - name: test
                    image: test:latest
            """;

    @Test
    void testAllInOneYamlProviderLoadsResources(@TempDir Path tempDir) throws IOException {
        Path manifestFile = tempDir.resolve("test-manifest.yaml");
        Files.writeString(manifestFile, TEST_MANIFEST);

        ManifestProvider provider = new AllInOneYamlProvider(manifestFile);
        List<HasMetadata> resources = provider.getResources();

        assertThat(resources)
                .hasSize(2)
                .anySatisfy(r -> assertThat(r)
                        .isInstanceOf(ConfigMap.class)
                        .hasFieldOrPropertyWithValue("metadata.name", "test-config"))
                .anySatisfy(r -> assertThat(r)
                        .isInstanceOf(Deployment.class)
                        .hasFieldOrPropertyWithValue("metadata.name", "test-deployment"));
    }

    @Test
    void testAllInOneYamlProviderHandlesMultipleResourceTypes(@TempDir Path tempDir) throws IOException {
        Path manifestFile = tempDir.resolve("test-manifest.yaml");
        Files.writeString(manifestFile, TEST_MANIFEST);

        ManifestProvider provider = new AllInOneYamlProvider(manifestFile);
        List<HasMetadata> resources = provider.getResources();

        assertThat(resources)
                .extracting(HasMetadata::getKind)
                .containsExactlyInAnyOrder("ConfigMap", "Deployment");
    }
}
