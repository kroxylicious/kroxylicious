/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

public class MirrorImageNameSubstitutor extends ImageNameSubstitutor {
    private static final Set<String> DOMAIN_ALLOW_LIST = Set.of("quay.io", "ghcr.io", "gcr.io", "redhat.com");
    private static final String MIRROR_REPOSITORY = "mirror.gcr.io";
    private static final Logger LOGGER = LoggerFactory.getLogger(MirrorImageNameSubstitutor.class);

    @Override
    public DockerImageName apply(DockerImageName dockerImageName) {
        if (DOMAIN_ALLOW_LIST.stream().anyMatch(domain -> dockerImageName.getRegistry().endsWith(domain))) {
            return dockerImageName;
        }
        else {
            DockerImageName substituted = dockerImageName.withRegistry(MIRROR_REPOSITORY);
            LOGGER.warn("replacing image repository for {} with mirror registry: {}", dockerImageName, substituted);
            return substituted;
        }
    }

    @Override
    protected String getDescription() {
        return "replaces image repository with mirror: " + MIRROR_REPOSITORY;
    }
}
