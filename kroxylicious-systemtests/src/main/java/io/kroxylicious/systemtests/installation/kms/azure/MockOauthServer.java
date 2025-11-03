/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.azure;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kms.azure.LowkeyVaultTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class MockOauthServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockOauthServer.class);
    public static final String MOCK_OAUTH_SERVER_SERVICE_NAME = "mock-oauth2-server";
    public static final String MOCK_OAUTH_SERVER_NODE_PORT_SERVICE_NAME = "mock-oauth2-server-" + Constants.NODE_PORT_TYPE.toLowerCase();
    public static final String MOCK_OAUTH_SERVER_CLUSTER_IP_SERVICE_NAME = "mock-oauth2-server-" + Constants.CLUSTER_IP_TYPE.toLowerCase();
    private static final String MOCK_OAUTH_SERVER_DEFAULT_NAMESPACE = "lowkey-vault";
    private static final String MOCK_OAUTH_SERVER_IMAGE = "ghcr.io/navikt/mock-oauth2-server:3.0.1";
    private final String deploymentNamespace;
    private static final String TENANT_ID = "tenant2";

    /**
     * Instantiates a new Mock Oauth Server.
     *
     */
    public MockOauthServer() {
        this.deploymentNamespace = MOCK_OAUTH_SERVER_DEFAULT_NAMESPACE;
    }

    public boolean isAvailable() {
        return !Environment.KMS_USE_CLOUD.equalsIgnoreCase("true");
    }

    private boolean isDeployed() {
        return kubeClient().getService(deploymentNamespace, MOCK_OAUTH_SERVER_NODE_PORT_SERVICE_NAME) != null;
    }

    /**
     * Deploy.
     */
    public void deploy() {
        if (isDeployed()) {
            LOGGER.warn("Skipping Mock Oauth Server deployment. It is already deployed!");
            return;
        }

        LOGGER.info("Deploy Mock Oauth Server in {} namespace", deploymentNamespace);

        NamespaceUtils.createNamespaceAndPrepare(deploymentNamespace);
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                LowkeyVaultTemplates.defaultMockOauthServerService(MOCK_OAUTH_SERVER_NODE_PORT_SERVICE_NAME, deploymentNamespace, MOCK_OAUTH_SERVER_SERVICE_NAME));
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                LowkeyVaultTemplates.defaultMockOauthServerClusterIPService(MOCK_OAUTH_SERVER_CLUSTER_IP_SERVICE_NAME, deploymentNamespace,
                        MOCK_OAUTH_SERVER_SERVICE_NAME));
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                LowkeyVaultTemplates.defaultMockOauthServerDeployment(MOCK_OAUTH_SERVER_SERVICE_NAME, MOCK_OAUTH_SERVER_IMAGE, deploymentNamespace));
    }

    /**
     * Delete.
     */
    public void delete() {
        LOGGER.info("Deleting Mock Oauth Server in {} namespace", deploymentNamespace);
        String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(deploymentNamespace, testSuiteName);
    }

    /**
     * Gets base uri.
     *
     * @return  the base uri
     */
    public URI getBaseUri() {
        return URI.create("http://" + MOCK_OAUTH_SERVER_CLUSTER_IP_SERVICE_NAME + ".lowkey-vault.svc.cluster.local:80");
    }

    /**
     * Gets tenant id.
     *
     * @return  the tenant id
     */
    public String getTenantId() {
        return TENANT_ID;
    }
}
