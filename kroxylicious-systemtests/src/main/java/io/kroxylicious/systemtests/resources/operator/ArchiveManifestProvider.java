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
 * Provides manifests from an extracted archive directory.
 * Supports the traditional approach where manifests are extracted from ZIP/TAR archives.
 */
public class ArchiveManifestProvider implements ManifestProvider {

    private final Path installDir;

    public ArchiveManifestProvider(Path installDir) {
        this.installDir = installDir;
    }

    @Override
    public List<HasMetadata> getResources() {
        List<HasMetadata> resources = new ArrayList<>();
        try (var fileStream = Files.list(installDir)) {
            fileStream.filter(Files::isRegularFile)
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
