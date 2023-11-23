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
        TestUtils.waitFor("namespace to be deleted", Constants.GLOBAL_POLL_INTERVAL_MILLIS, Constants.GLOBAL_TIMEOUT_MILLIS,
                () -> kubeClient().getNamespace(namespace) == null);

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
        TestUtils.waitFor("namespace to be created", Constants.GLOBAL_POLL_INTERVAL_MILLIS, Constants.GLOBAL_TIMEOUT_MILLIS,
                () -> kubeClient().getNamespace(namespace) != null);

        LOGGER.info("Namespace: {} created", namespace);
    }
}
