/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.installation.strimzi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.Constants;
import io.kroxylicious.executor.Exec;
import io.kroxylicious.utils.DeploymentUtils;

public class Strimzi {

    private static final Logger LOGGER = LoggerFactory.getLogger(Strimzi.class);

    private String deploymentNamespace;

    public Strimzi(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
    }

    public void deploy() {
        LOGGER.info("Deploy Strimzi in {} namespace", deploymentNamespace);
        Exec.exec("kubectl", "apply", "-f", "https://strimzi.io/install/latest?namespace=" + deploymentNamespace, "-n", deploymentNamespace);
        DeploymentUtils.waitForDeploymentReady(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }

    public void delete() {
        LOGGER.info("Deleting Strimzi in {} namespace", deploymentNamespace);
        Exec.exec("kubectl", "delete", "-f", "https://strimzi.io/install/latest?namespace=" + deploymentNamespace, "-n", deploymentNamespace);
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }
}
