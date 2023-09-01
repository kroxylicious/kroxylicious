/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.utils.kubeUtils.objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.strimzi.test.TestUtils;

import io.kroxylicious.systemtest.Constants;
import io.kroxylicious.systemtest.resources.ResourceOperation;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NamespaceUtils {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private NamespaceUtils() {
    }

    public static void waitForNamespaceDeletion(String name) {
        LOGGER.info("Waiting for Namespace: {} deletion", name);

        TestUtils.waitFor("Namespace: " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
                () -> kubeClient().getNamespace(name) == null);
        LOGGER.info("Namespace: {} was deleted", name);
    }
}
