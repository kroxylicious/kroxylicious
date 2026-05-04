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
 * Runs the plugin end-to-end test on a dedicated Kind cluster with the
 * {@code ImageVolume} feature gate enabled.
 */
@EnabledIf("io.kroxylicious.kubernetes.webhook.KindPluginEndToEndKT#isEnvironmentValid")
class KindPluginEndToEndKT extends AbstractPluginEndToEndKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KindPluginEndToEndKT.class);

    private static final String KIND_CLUSTER_NAME = "kroxy-plugin-test";
    private static final String KIND_CONTEXT = "kind-" + KIND_CLUSTER_NAME;

    @Override
    String kubeContext() {
        return KIND_CONTEXT;
    }

    @BeforeAll
    static void createKindCluster() {
        LOGGER.info("Creating Kind cluster '{}' with ImageVolume feature gate", KIND_CLUSTER_NAME);
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kind create cluster --name %s --config=-
                kind: Cluster
                apiVersion: kind.x-k8s.io/v1alpha4
                featureGates:
                  ImageVolume: true
                nodes:
                  - role: control-plane
                EOF""".formatted(KIND_CLUSTER_NAME));

        LOGGER.info("Loading images into Kind cluster");
        ShellUtils.exec("kind", "load", "image-archive", INFO.imageArchive(),
                "--name", KIND_CLUSTER_NAME);
        ShellUtils.exec("kind", "load", "image-archive", INFO.proxyImageArchive(),
                "--name", KIND_CLUSTER_NAME);
        ShellUtils.exec("kind", "load", "image-archive", INFO.testPluginImageArchive(),
                "--name", KIND_CLUSTER_NAME);
    }

    @AfterAll
    static void deleteKindCluster() {
        LOGGER.info("Deleting Kind cluster '{}'", KIND_CLUSTER_NAME);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kind", "delete", "cluster", "--name", KIND_CLUSTER_NAME);
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("kind", "openssl")
                && allImageArchivesExist();
    }
}
