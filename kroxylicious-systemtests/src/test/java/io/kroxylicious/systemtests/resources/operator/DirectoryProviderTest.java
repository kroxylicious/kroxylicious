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

class DirectoryProviderTest {

    private static final String CONFIG_MAP_YAML = """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: test-config
              namespace: test-ns
            data:
              key: value
            """;

    private static final String DEPLOYMENT_YAML = """
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
    void testDirectoryManifestProviderLoadsResources(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("01-configmap.yaml"), CONFIG_MAP_YAML);
        Files.writeString(tempDir.resolve("02-deployment.yaml"), DEPLOYMENT_YAML);

        ManifestProvider provider = new DirectoryManifestProvider(tempDir);
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
    void testDirectoryManifestProviderIgnoresNonYamlFiles(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("01-configmap.yaml"), CONFIG_MAP_YAML);
        Files.writeString(tempDir.resolve("02-deployment.yaml"), DEPLOYMENT_YAML);
        Files.writeString(tempDir.resolve("README.md"), "# This is not a manifest");

        ManifestProvider provider = new DirectoryManifestProvider(tempDir);
        List<HasMetadata> resources = provider.getResources();

        assertThat(resources)
                .hasSize(2)
                .extracting(HasMetadata::getKind)
                .containsExactlyInAnyOrder("ConfigMap", "Deployment");
    }
}
