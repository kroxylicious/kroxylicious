/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.fabric8.kubernetes.api.model.ContainerBuilder;

import io.kroxylicious.systemtests.Constants;

public class ContainerTemplates {

    private static final List<String> SNAPSHOT_STRINGS = List.of("latest", "snapshot");

    private static final Set<String> snapshotImagesPulledOnce = new HashSet<>();

    /**
     * If the image contains the string 'latest' we set imagePullPolicy to Always for the first
     * Container built and then IfNotPresent thereafter (process-scoped). This is to help ensure
     * we have the latest image during local development.
     *
     * @param containerName container name
     * @param image         image
     * @return container builder
     */
    public synchronized static ContainerBuilder baseImageBuilder(String containerName, String image) {
        var imagePullPolicy = Constants.PULL_IMAGE_IF_NOT_PRESENT;
        String lowerCaseImage = image.toLowerCase();
        if (SNAPSHOT_STRINGS.stream().anyMatch(lowerCaseImage::contains) && !snapshotImagesPulledOnce.contains(image)) {
            imagePullPolicy = Constants.PULL_IMAGE_ALWAYS;
            snapshotImagesPulledOnce.add(image);
        }
        return new ContainerBuilder()
                                     .withName(containerName)
                                     .withImage(image)
                                     .withImagePullPolicy(imagePullPolicy);
    }
}
