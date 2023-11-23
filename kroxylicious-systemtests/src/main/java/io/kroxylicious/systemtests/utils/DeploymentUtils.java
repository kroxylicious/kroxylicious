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
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.systemtests.Constants;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Deployment utils.
 */
public class DeploymentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentUtils.class);

    private static final long READINESS_TIMEOUT = Duration.ofMinutes(6).toMillis();
    private static final long DELETION_TIMEOUT = Duration.ofMinutes(5).toMillis();
    private static final String TEST_LOAD_BALANCER_NAME = "test-load-balancer";

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
                () -> kubeClient(namespaceName).isDeploymentReady(namespaceName, deploymentName));

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

    /**
     * Gets deployment file from url.
     *
     * @param url the url
     * @return the deployment file from url
     * @throws IOException the io exception
     */
    public static FileInputStream getDeploymentFileFromURL(String url) throws IOException {
        File deploymentFile = Files.createTempFile("deploy", ".yaml", TestUtils.getDefaultPosixFilePermissions()).toFile();
        FileUtils.copyURLToFile(
                new URL(url),
                deploymentFile,
                2000,
                5000);
        deploymentFile.deleteOnExit();

        return new FileInputStream(deploymentFile);
    }

    /**
     * Check load balancer is working.
     *
     * @param namespace the namespace
     * @return the boolean
     */
    public static boolean checkLoadBalancerIsWorking(String namespace) {
        Service service = new ServiceBuilder()
                .withKind(Constants.SERVICE_KIND)
                .withNewMetadata()
                .withName(TEST_LOAD_BALANCER_NAME)
                .withNamespace(namespace)
                .addToLabels("app", "loadbalancer")
                .endMetadata()
                .withNewSpec()
                .addNewPort()
                .withPort(8080)
                .endPort()
                .withSelector(Collections.singletonMap("app", "loadbalancer"))
                .withType(Constants.LOAD_BALANCER_TYPE)
                .endSpec()
                .build();
        kubeClient().getClient().services().inNamespace(namespace).resource(service).create();
        boolean isWorking;
        try {
            LOGGER.debug("Waiting for the ingress IP to be available...");
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> !kubeClient().getService(namespace, TEST_LOAD_BALANCER_NAME).getStatus().getLoadBalancer().getIngress().isEmpty()
                            && kubeClient().getService(namespace, TEST_LOAD_BALANCER_NAME).getStatus().getLoadBalancer().getIngress().get(0).getIp() != null);
            isWorking = true;
        }
        catch (Exception e) {
            isWorking = false;
            LOGGER.warn("The ingress IP is not available!");
            LOGGER.warn(e.getMessage());
        }
        kubeClient().getClient().apps().deployments().inNamespace(namespace).withName(TEST_LOAD_BALANCER_NAME).delete();
        return isWorking;
    }
}
