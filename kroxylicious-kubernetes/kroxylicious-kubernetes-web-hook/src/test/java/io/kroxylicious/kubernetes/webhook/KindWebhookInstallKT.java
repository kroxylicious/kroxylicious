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
 * Webhook installation and sidecar injection test on a Kind cluster.
 * Requires a running Kind cluster and the webhook container image archive
 * (built by the {@code dist} Maven profile).
 */
@EnabledIf("io.kroxylicious.kubernetes.webhook.KindWebhookInstallKT#isEnvironmentValid")
class KindWebhookInstallKT extends AbstractWebhookInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KindWebhookInstallKT.class);

    private static final String IMAGE_NAME = WebhookInfo.fromResource().imageName();
    private static final String IMAGE_ARCHIVE = WebhookInfo.fromResource().imageArchive();

    @BeforeAll
    static void beforeAll() {
        LOGGER.info("Importing {} into kind", IMAGE_NAME);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kind", "load", "image-archive", IMAGE_ARCHIVE);
    }

    @AfterAll
    static void afterAll() {
        // Kind does not provide a clean way to remove images
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("kind", "openssl")
                && validateKubeContext("kind-kind")
                && testImageAvailable();
    }
}
