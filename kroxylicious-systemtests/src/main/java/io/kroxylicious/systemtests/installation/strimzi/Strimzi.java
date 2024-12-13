/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.strimzi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.ResourceManager;
import io.kroxylicious.systemtests.utils.ReadWriteUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Strimzi.
 */
public class Strimzi {
    private static final Logger LOGGER = LoggerFactory.getLogger(Strimzi.class);

    public static final String STRIMZI_HELM_REPOSITORY = "quay.io/strimzi-helm/strimzi-kafka-operator";
    public static final String STRIMZI_SERVICE_NAME = "strimzi-cluster-operator";
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
     */
    public void deploy() {
        LOGGER.info("Deploy Strimzi in {} namespace", deploymentNamespace);
        if (kubeClient().getDeployment(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME) != null
                || Environment.SKIP_STRIMZI_INSTALL) {
            LOGGER.warn("Skipping strimzi deployment. It is already deployed!");
            return;
        }

        ResourceManager.helmClient().namespace(deploymentNamespace).installByContainerImage(STRIMZI_HELM_REPOSITORY, STRIMZI_SERVICE_NAME,
                Optional.of(Environment.STRIMZI_VERSION),
                Optional.of(Path.of(ReadWriteUtils.getResourceURI(getClass(), "helm_strimzi_overrides.yaml"))),
                Optional.empty());
    }

    /**
     * Delete strimzi.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        if (Environment.SKIP_STRIMZI_INSTALL) {
            LOGGER.warn("Skipping Strimzi deletion. SKIP_STRIMZI_INSTALL was set to true");
            return;
        }
        LOGGER.info("Deleting Strimzi in {} namespace", deploymentNamespace);
        ResourceManager.helmClient().namespace(deploymentNamespace).delete(STRIMZI_SERVICE_NAME);
    }
}
