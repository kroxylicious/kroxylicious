/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.metrics;

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.templates.ContainerTemplates;

public class ScraperTemplates {

    private ScraperTemplates() {
    }

    public static DeploymentBuilder scraperPod(String namespaceName, String podName) {
        Map<String, String> label = new HashMap<>();

        label.put(Constants.SCRAPER_LABEL_KEY, Constants.SCRAPER_LABEL_VALUE);
        label.put(Constants.DEPLOYMENT_TYPE, Constants.SCRAPER_NAME);
        String kroxyRepoUrl = Environment.KROXY_IMAGE_REPO + (Environment.KROXY_IMAGE_REPO.endsWith(":") ? "" : ":");
        String scraperImage = kroxyRepoUrl + Environment.KROXY_VERSION;

        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(podName)
                .withLabels(label)
                .withNamespace(namespaceName)
                .endMetadata()
                .withNewSpec()
                .withNewSelector()
                .addToMatchLabels("app", podName)
                .addToMatchLabels(label)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("app", podName)
                .addToLabels(label)
                .endMetadata()
                .withNewSpec()
                .withContainers(
                        ContainerTemplates.baseImageBuilder(podName, scraperImage)
                                .withCommand("sleep")
                                .withArgs("infinity")
                                .build())
                .withImagePullSecrets(new LocalObjectReferenceBuilder()
                        .withName("regcred")
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }
}
