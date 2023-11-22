/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousDeploymentTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousServiceTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kroxylicious.
 */
public class Kroxylicious {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxylicious.class);
    private final String deploymentNamespace;
    private final String containerImage;
    private final ResourceManager resourceManager = ResourceManager.getInstance();

    /**
     * Instantiates a new KroxyliciousService to be used in kubernetes.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Kroxylicious(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
        String kroxyUrl = Environment.KROXY_IMAGE_REPO + (Environment.KROXY_IMAGE_REPO.endsWith(":") ? "" : ":");
        this.containerImage = kroxyUrl + Environment.KROXY_VERSION;
    }

    /**
     * Deploy - Port per broker plain config
     * @param clusterName the cluster name
     * @param replicas the replicas
     */
    public void deployPortPerBrokerPlain(String clusterName, int replicas) {
        LOGGER.info("Deploy Kroxylicious in {} namespace", deploymentNamespace);
        resourceManager.createResourceWithWait(KroxyliciousConfigTemplates.defaultKroxyConfig(clusterName, deploymentNamespace).build());
        resourceManager.createResourceWithWait(KroxyliciousDeploymentTemplates.defaultKroxyDeployment(deploymentNamespace, containerImage, replicas).build());
        resourceManager.createResourceWithoutWait(KroxyliciousServiceTemplates.defaultKroxyService(deploymentNamespace).build());
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
    public String getBootstrap() {
        String clusterIP = kubeClient().getService(deploymentNamespace, Constants.KROXY_SERVICE_NAME).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException(new Throwable("Unable to get the clusterIP of Kroxylicious"));
        }
        String bootstrap = clusterIP + ":9292";
        LOGGER.debug("Kroxylicious bootstrap: {}", bootstrap);
        return bootstrap;
    }
}
