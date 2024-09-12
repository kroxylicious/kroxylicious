/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Namespace utils.
 */
public class NamespaceUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceUtils.class);

    /**
     * Delete namespace with wait.
     *
     * @param namespace the namespace
     */
    public static void deleteNamespaceWithWait(String namespace) {
        LOGGER.info("Deleting namespace: {}", namespace);
        kubeClient().deleteNamespace(namespace);
        await().atMost(Constants.GLOBAL_TIMEOUT)
               .pollInterval(Constants.GLOBAL_POLL_INTERVAL)
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
        await().atMost(Constants.GLOBAL_TIMEOUT)
               .pollInterval(Constants.GLOBAL_POLL_INTERVAL)
               .until(() -> kubeClient().getNamespace(namespace) != null);

        LOGGER.info("Namespace: {} created", namespace);
    }
}
