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
import java.util.ArrayList;
import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.skodjob.kubetest4j.utils.KubeTestUtils;

/**
 * Provides manifests from a directory of individual YAML files.
 * Supports the traditional approach where manifests are extracted from ZIP/TAR archives or provided as separate files.
 */
public class DirectoryManifestProvider implements ManifestProvider {

    private final Path installDir;

    public DirectoryManifestProvider(Path installDir) {
        this.installDir = installDir;
    }

    @Override
    public List<HasMetadata> getResources() {
        List<HasMetadata> resources = new ArrayList<>();
        try (var fileStream = Files.list(installDir)) {
            fileStream.filter(Files::isRegularFile)
                    .filter(path -> {
                        String filename = path.getFileName().toString();
                        return filename.endsWith(".yaml") || filename.endsWith(".yml");
                    })
                    .sorted()
                    .forEach(path -> {
                        var loadedResources = KubeTestUtils.configFromYaml(path.toFile(), HasMetadata.class);
                        if (loadedResources != null) {
                            resources.add(loadedResources);
                        }
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return resources;
    }
}
