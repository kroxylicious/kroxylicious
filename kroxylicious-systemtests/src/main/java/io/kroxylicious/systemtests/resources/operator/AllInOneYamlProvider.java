/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

/**
 * Provides manifests from a single all-in-one YAML file.
 * Supports the GitOps approach where a single manifest file contains all resources including CRDs.
 */
public class AllInOneYamlProvider implements ManifestProvider {

    private final Path yaml;

    public AllInOneYamlProvider(Path yaml) {
        this.yaml = yaml;
    }

    @Override
    public List<HasMetadata> getResources() {
        List<HasMetadata> resources = new ArrayList<>();
        try (InputStream is = Files.newInputStream(yaml)) {
            var loadedResources = new KubernetesClientBuilder().build().load(is).get();
            resources.addAll(loadedResources);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to load manifest from " + yaml, e);
        }
        return resources;
    }
}
