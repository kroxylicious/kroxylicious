/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import io.kroxylicious.systemtests.Constants;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Deployment utils.
 */
public class DeploymentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentUtils.class);

    private static final long READINESS_TIMEOUT = Duration.ofMinutes(6).toMillis();
    private static final long DELETION_TIMEOUT = Duration.ofMinutes(5).toMillis();

    /**
     * Wait for deployment ready.
     *
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     */
    public static void waitForDeploymentReady(String namespaceName, String deploymentName) {
        LOGGER.info("Waiting for Deployment: {}/{} to be ready", namespaceName, deploymentName);

        TestUtils.waitFor("readiness of Deployment: " + namespaceName + "/" + deploymentName,
                Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS_MILLIS, READINESS_TIMEOUT,
                () -> kubeClient(namespaceName).getDeploymentStatus(namespaceName, deploymentName));

        LOGGER.info("Deployment: {}/{} is ready", namespaceName, deploymentName);
    }

    /**
     * Wait for pod to be ready by label.
     *
     * @param namespaceName the namespace name
     * @param labelKey the label key
     * @param labelValue the label value
     */
    public static void waitForPodToBeReadyByLabel(String namespaceName, String labelKey, String labelValue) {
        LOGGER.info("Waiting for Pods: {}/{} to be ready", namespaceName, labelValue);
        TestUtils.waitFor("readiness of Pod: " + namespaceName + "/" + labelValue,
                Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS_MILLIS, READINESS_TIMEOUT,
                () -> !kubeClient().getClient().pods().inNamespace(namespaceName).withLabel(labelKey, labelValue).list().getItems().isEmpty());
        kubeClient().getClient().pods().inNamespace(namespaceName).withLabel(labelKey, labelValue).waitUntilReady(READINESS_TIMEOUT, TimeUnit.MILLISECONDS);
        LOGGER.info("Pod: {}/{} is ready", namespaceName, labelValue);
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

    /**
     * Gets deployment file from url.
     *
     * @param url the url
     * @return the deployment file from url
     * @throws IOException the io exception
     */
    public static FileInputStream getDeploymentFileFromURL(String url) throws IOException {
        String deploymentFile = "/tmp/deploy.yaml";
        File file = new File(deploymentFile);
        FileUtils.copyURLToFile(
                new URL(url),
                file,
                2000,
                5000);
        file.deleteOnExit();

        return new FileInputStream(deploymentFile);
    }
}
