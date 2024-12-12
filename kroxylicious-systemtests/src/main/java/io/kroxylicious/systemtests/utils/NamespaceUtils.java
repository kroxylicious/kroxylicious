/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Namespace utils.
 */
public class NamespaceUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceUtils.class);

    private NamespaceUtils() {
    }

    /**
     * Delete namespace with wait.
     *
     * @param namespace the namespace
     */
    public static void deleteNamespaceWithWait(String namespace) {
        LOGGER.info("Deleting namespace: {}", namespace);
        kubeClient().deleteNamespace(namespace);
        await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(() -> kubeClient().getNamespace(namespace) == null);

        LOGGER.info("Namespace: {} deleted", namespace);
    }

    /**
     * Create namespace with wait.
     *
     * @param namespace the namespace
     */
    public static void createNamespaceWithWait(String namespace) {
        LOGGER.info("Creating namespace: {}", namespace);
        if (kubeClient().getNamespace(namespace) != null) {
            LOGGER.warn("Namespace was already created!");
            return;
        }
        kubeClient().createNamespace(namespace);
        await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(() -> kubeClient().getNamespace(namespace) != null);

        LOGGER.info("Namespace: {} created", namespace);
    }

    /**
     * Method does following:
     *  - creates Namespace and waits for its readiness
     *  - applies default NetworkPolicy settings
     *  - copies image pull secrets from `default` Namespace
     *
     * @param namespaceName name of the Namespace that should be created and prepared
     */
    public static void createNamespaceAndPrepare(String namespaceName) {
        createNamespaceWithWait(namespaceName);
        DeploymentUtils.registryCredentialsSecret(namespaceName);
    }

    /**
     * Creates and prepares all Namespaces from {@param namespacesToBeCreated}.
     * After that sets Namespace in {@link KubeClusterResource} to {@param useNamespace}.
     *
     * @param useNamespace Namespace name that should be used in {@link KubeClusterResource}
     * @param namespacesToBeCreated list of Namespaces that should be created
     */
    public static void createNamespaces(String useNamespace, List<String> namespacesToBeCreated) {
        namespacesToBeCreated.forEach(NamespaceUtils::createNamespaceAndPrepare);
        KubeClusterResource.getInstance().setNamespace(useNamespace);
    }
}