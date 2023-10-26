/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.IOException;
import java.util.Objects;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyConfigTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyDeploymentTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyServiceTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kroxy.
 */
public class Kroxylicious {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxylicious.class);
    private final String deploymentNamespace;
    private final String containerImage;
    private final ResourceManager resourceManager = ResourceManager.getInstance();

    /**
     * Instantiates a new Kroxylicious.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Kroxylicious(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
        String kroxyUrl = Environment.KROXY_IMAGE_REPO + (Environment.KROXY_IMAGE_REPO.endsWith(":") ? "" : ":");
        if (!Objects.equals(Environment.QUAY_ORG, Environment.QUAY_ORG_DEFAULT)) {
            kroxyUrl = "quay.io/" + Environment.QUAY_ORG + "/kroxylicious:";
        }
        containerImage = kroxyUrl + Environment.KROXY_VERSION;
    }

    /**
     * Deploy - Port per broker plain config
     * @param displayName the display name
     * @param clusterName the cluster name
     * @param replicas the replicas
     */
    public void deployPortPerBrokerPlain(String displayName, String clusterName, int replicas) {
        LOGGER.info("Deploy Kroxy in {} namespace", deploymentNamespace);
        resourceManager.createResourceWithWait(displayName, KroxyConfigTemplates.defaultKroxyConfig(clusterName, deploymentNamespace).build());
        resourceManager.createResourceWithWait(displayName, KroxyDeploymentTemplates.defaultKroxyDeployment(deploymentNamespace, containerImage, replicas).build());
        resourceManager.createResourceWithoutWait(displayName, KroxyServiceTemplates.defaultKroxyService(deploymentNamespace).build());
    }

    /**
     * Delete.
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    public void delete(TestInfo testInfo) throws IOException {
        LOGGER.info("Deleting Kroxy in {} namespace", deploymentNamespace);
        resourceManager.deleteResources(testInfo.getDisplayName());
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME);
    }

    /**
     * Gets number of replicas.
     *
     * @return the number of replicas
     */
    public int getNumberOfReplicas() {
        LOGGER.info("Getting number of replicas..");
        return kubeClient().getDeployment(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME).getStatus().getReplicas();
    }

    /**
     * Get bootstrap string.
     *
     * @return the bootstrap
     */
    public String getBootstrap(){
        String clusterIP = kubeClient().getService(deploymentNamespace, Constants.KROXY_SERVICE_NAME).getSpec().getClusterIP();
        return clusterIP + ":9292";
    }
}
