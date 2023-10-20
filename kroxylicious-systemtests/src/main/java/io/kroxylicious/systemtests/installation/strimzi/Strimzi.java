/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.strimzi;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Strimzi.
 */
public class Strimzi {
    private static final Logger LOGGER = LoggerFactory.getLogger(Strimzi.class);
    private final String deploymentNamespace;

    /**
     * Instantiates a new Strimzi.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Strimzi(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
    }

    /**
     * Deploy strimzi.
     * @throws IOException the io exception
     */
    public void deploy() throws IOException {
        LOGGER.info("Deploy Strimzi in {} namespace", deploymentNamespace);
        kubeClient().getClient().load(DeploymentUtils.getDeploymentFileFromURL(Environment.STRIMZI_URL))
                .inNamespace(deploymentNamespace)
                .create();
        DeploymentUtils.waitForDeploymentReady(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }

    /**
     * Delete strimzi.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        LOGGER.info("Deleting Strimzi in {} namespace", deploymentNamespace);
        kubeClient().getClient().load(DeploymentUtils.getDeploymentFileFromURL(Environment.STRIMZI_URL))
                .inNamespace(deploymentNamespace)
                .delete();
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }
}
