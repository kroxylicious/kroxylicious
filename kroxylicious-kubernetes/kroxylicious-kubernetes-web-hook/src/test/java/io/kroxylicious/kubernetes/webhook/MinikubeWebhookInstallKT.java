/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.test.ShellUtils;

/**
 * Webhook installation and sidecar injection test on a Minikube cluster.
 * Requires a running Minikube cluster and the webhook container image archive
 * (built by the {@code dist} Maven profile).
 */
@EnabledIf("io.kroxylicious.kubernetes.webhook.MinikubeWebhookInstallKT#isEnvironmentValid")
class MinikubeWebhookInstallKT extends AbstractWebhookInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinikubeWebhookInstallKT.class);

    private static final String IMAGE_NAME = WebhookInfo.fromResource().imageName();
    private static final String IMAGE_ARCHIVE = WebhookInfo.fromResource().imageArchive();

    private static boolean loaded = false;

    @BeforeAll
    static void beforeAll() {
        LOGGER.info("Loading {} into minikube registry", IMAGE_ARCHIVE);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", IMAGE_ARCHIVE);
        loaded = true;
    }

    @AfterAll
    static void afterAll() {
        if (loaded) {
            LOGGER.info("Removing {} from minikube registry", IMAGE_NAME);
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                    "minikube", "image", "rm", IMAGE_NAME);
        }
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("minikube", "openssl")
                && validateKubeContext("minikube")
                && testImageAvailable();
    }
}
