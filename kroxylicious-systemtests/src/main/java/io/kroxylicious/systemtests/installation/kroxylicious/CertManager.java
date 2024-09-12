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
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Cert manager.
 */
public class CertManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CertManager.class);

    private final NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> deployment;
    private boolean deleteCertManager = true;

    /**
     * Instantiates a new Cert manager.
     *
     * @throws IOException the io exception
     */
    public CertManager() throws IOException {
        deployment = kubeClient().getClient()
                                 .load(DeploymentUtils.getDeploymentFileFromURL(Constants.CERT_MANAGER_URL));
    }

    private boolean isDeployed() {
        return !kubeClient().getClient().apps().deployments().inAnyNamespace().withLabelSelector("app=cert-manager").list().getItems().isEmpty();
    }

    /**
     * Deploy cert manager.
     */
    public void deploy() {
        LOGGER.info("Deploy cert manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        if (isDeployed()) {
            LOGGER.warn("Skipping cert manager deployment. It is already deployed!");
            deleteCertManager = false;
            return;
        }
        deployment.create();
        DeploymentUtils.waitForDeploymentReady(Constants.CERT_MANAGER_NAMESPACE, "cert-manager-webhook");
    }

    /**
     * Delete cert manager.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        if (!deleteCertManager) {
            LOGGER.warn("Skipping cert manager deletion. It was previously installed");
            return;
        }
        LOGGER.info("Deleting Cert Manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        deployment.withGracePeriod(0).delete();
        NamespaceUtils.deleteNamespaceWithWait(Constants.CERT_MANAGER_NAMESPACE);
    }
}
