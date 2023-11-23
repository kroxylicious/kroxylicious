/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Cert manager.
 */
public class CertManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CertManager.class);

    private final NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> deployment;

    /**
     * Instantiates a new Cert manager.
     *
     * @throws IOException the io exception
     */
    public CertManager() throws IOException {
        deployment = kubeClient().getClient()
                .load(DeploymentUtils.getDeploymentFileFromURL(Constants.CERT_MANAGER_URL));
    }

    /**
     * Deploy cert manager.
     */
    public void deploy() {
        LOGGER.info("Deploy cert manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        deployment.create();
        DeploymentUtils.waitForDeploymentReady(Constants.CERT_MANAGER_NAMESPACE, "cert-manager-webhook");
    }

    /**
     * Delete cert manager.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        LOGGER.info("Deleting Cert Manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        deployment.withGracePeriod(0).delete();
    }
}
