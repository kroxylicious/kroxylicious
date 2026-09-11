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

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.kroxylicious.testing.integration.ShellUtils;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the plugin end-to-end tests on an existing Minikube cluster.
 * Requires a running Minikube cluster (K8s >= 1.35 for ImageVolume support)
 * and the webhook, proxy, and test-plugin container image archives
 * (built by the {@code dist} Maven profile).
 */
@EnabledIf("io.kroxylicious.kubernetes.webhook.MinikubePluginEndToEndKT#isEnvironmentValid")
class MinikubePluginEndToEndKT extends AbstractPluginEndToEndKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinikubePluginEndToEndKT.class);

    private boolean loaded = false;

    @Override
    String kubeContext() {
        return "minikube";
    }

    @BeforeAll
    void setUp() {
        checkMinikubeKubernetesVersion();

        LOGGER.info("Loading images into minikube registry");
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", INFO.imageArchive());
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", INFO.proxyImageArchive());
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "minikube", "image", "load", INFO.testPluginImageArchive());
        loaded = true;

        initClientAndInstallStrimzi();
    }

    @AfterAll
    void tearDown() {
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

    private static void checkMinikubeKubernetesVersion() {
        Config config = Config.autoConfigure("minikube");
        try (KubernetesClient c = new KubernetesClientBuilder()
                .withConfig(config).build()) {
            var info = c.getKubernetesVersion();
            int minor = Integer.parseInt(info.getMinor().replaceAll("[^0-9]", ""));
            assumeTrue(minor >= 35,
                    "Minikube Kubernetes version is 1." + minor
                            + " but >= 1.35 is required (ImageVolume enabled by default)."
                            + " Restart minikube with a newer version:"
                            + " minikube start --kubernetes-version=v1.35.0");
        }
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("minikube", "openssl")
                && validateKubeContext("minikube")
                && allImageArchivesExist();
    }
}
