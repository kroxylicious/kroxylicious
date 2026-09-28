/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provides manifests from a single all-in-one YAML file.
 * Supports the GitOps approach where a single manifest file contains all resources including CRDs.
 */
public class AllInOneYamlManifestProvider implements ManifestProvider {

    @NonNull
    private final Path yaml;

    public AllInOneYamlManifestProvider(@NonNull Path yaml) {
        this.yaml = Objects.requireNonNull(yaml);
    }

    @Override
    public List<HasMetadata> getResources() {
        try (var is = Files.newInputStream(yaml);
                var client = new KubernetesClientBuilder().build()) {
            return client.load(is).items();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to load manifest from " + yaml, e);
        }
    }
}
