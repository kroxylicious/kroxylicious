/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.utils;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.Constants;

import static io.kroxylicious.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.k8s.KubeClusterResource.kubeClient;

public class DeploymentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentUtils.class);

    private static final long READINESS_TIMEOUT = Duration.ofMinutes(8).toMillis();
    private static final long DELETION_TIMEOUT = Duration.ofMinutes(5).toMillis();

    public static void waitForDeploymentReady(String namespaceName, String deploymentName) {
        LOGGER.info("Waiting for Deployment: {}/{} to be ready", namespaceName, deploymentName);

        TestUtils.waitFor("readiness of Deployment: " + namespaceName + "/" + deploymentName,
                Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS_MILLIS, READINESS_TIMEOUT,
                () -> kubeClient(namespaceName).getDeploymentStatus(namespaceName, deploymentName));

        LOGGER.info("Deployment: {}/{} is ready", namespaceName, deploymentName);
    }

    /**
     * Wait until the given Deployment has been deleted.
     * @param namespaceName Namespace name
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentDeletion(String namespaceName, String name) {
        LOGGER.debug("Waiting for Deployment: {}/{} deletion", namespaceName, name);
        TestUtils.waitFor("deletion of Deployment: " + namespaceName + "/" + name, Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION_MILLIS, DELETION_TIMEOUT,
                () -> {
                    if (kubeClient(namespaceName).getDeployment(namespaceName, name) == null) {
                        return true;
                    }
                    else {
                        LOGGER.warn("Deployment: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespaceName, name);
                        cmdKubeClient(namespaceName).deleteByName(Constants.DEPLOYMENT, name);
                        return false;
                    }
                });
        LOGGER.debug("Deployment: {}/{} was deleted", namespaceName, name);
    }
}
