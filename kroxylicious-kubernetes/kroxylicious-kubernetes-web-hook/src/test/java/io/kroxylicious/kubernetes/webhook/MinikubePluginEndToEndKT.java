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
 * Runs the plugin end-to-end test on an existing Minikube cluster.
 * Requires a running Minikube cluster and the webhook, proxy, and test-plugin
 * container image archives (built by the {@code dist} Maven profile).
 */
@EnabledIf("io.kroxylicious.kubernetes.webhook.MinikubePluginEndToEndKT#isEnvironmentValid")
class MinikubePluginEndToEndKT extends AbstractPluginEndToEndKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinikubePluginEndToEndKT.class);

    private static boolean loaded = false;

    @Override
    String kubeContext() {
        return "minikube";
    }

    @BeforeAll
    static void loadImages() {
        LOGGER.info("Loading images into minikube registry");
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", INFO.imageArchive());
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", INFO.proxyImageArchive());
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", INFO.testPluginImageArchive());
        loaded = true;
    }

    @AfterAll
    static void removeImages() {
        if (loaded) {
            LOGGER.info("Removing images from minikube registry");
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                    "minikube", "image", "rm", INFO.imageName());
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                    "minikube", "image", "rm", INFO.proxyImageName());
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                    "minikube", "image", "rm", INFO.testPluginImageName());
        }
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("minikube", "openssl")
                && validateKubeContext("minikube")
                && allImageArchivesExist();
    }
}
