/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.testing.integration.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * An abstract test that we can install the operator.
 * Abstract because this class only depends on kubectl.
 * It's not defined here how a Kube cluster is provided or how it knows about the images we're testing.
 */
abstract class AbstractInstallKT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInstallKT.class);
    static final Predicate<Stream<String>> ALWAYS_VALID = lines -> true;

    static boolean testImageAvailable() {
        String imageArchive = OperatorInfo.fromResource().imageArchive();
        assumeThat(Path.of(imageArchive))
                .describedAs("Container image archive %s must exist", imageArchive)
                .withFailMessage("Container image archive %s did not exist", imageArchive)
                .exists();
        return true;
    }

    @Test
    void shouldInstallFromYamlManifests() {
        try {
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", "target/packaged/install")).isTrue();

            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-operator", "--for=jsonpath={.status.readyReplicas}=1",
                    "--timeout=300s", "deployment", "kroxylicious-operator")).isTrue();
            LOGGER.info("Operator deployment became ready");
        }
        finally {
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "delete", "-f", "target/packaged/install");
        }
    }

    @Test
    void shouldInstallFromRenderedManifest() {
        Path manifest = getFullInstallManifest();
        try {
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", manifest.toString())).isTrue();

            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-operator",
                    "--for=jsonpath={.status.readyReplicas}=1", "--timeout=300s", "deployment", "kroxylicious-operator")).isTrue();
            LOGGER.info("Operator deployment became ready from rendered install manifest");
        }
        finally {
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "delete", "-f", manifest.toString());
        }
    }

    @Test
    void shouldInstallFromCrdsOnlyThenOperator() {
        Path crdsManifest = getCrdsOnlyManifest();
        Path fullManifest = getFullInstallManifest();

        try {
            // Install CRDs first
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", crdsManifest.toString())).isTrue();
            LOGGER.info("Applied CRDs-only manifest");

            // Verify CRDs are established
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "--for=condition=Established",
                    "--timeout=60s", "crd", "kafkaproxies.kroxylicious.io")).isTrue();
            LOGGER.info("CRDs became established");

            // Then install full manifest (should work even though CRDs already exist)
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", fullManifest.toString())).isTrue();

            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-operator",
                    "--for=jsonpath={.status.readyReplicas}=1", "--timeout=300s", "deployment", "kroxylicious-operator")).isTrue();
            LOGGER.info("Operator deployment became ready after two-stage installation");
        }
        finally {
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "delete", "-f", fullManifest.toString());
        }
    }

    private Path getFullInstallManifest() {
        String version = OperatorInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-operator-install-" + version + ".yaml");
        assumeThat(manifest)
                .describedAs("Full install manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    private Path getCrdsOnlyManifest() {
        String version = OperatorInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-operator-crds-" + version + ".yaml");
        assumeThat(manifest)
                .describedAs("CRDs-only manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    static boolean validateKubeContext(String expectedContext) {
        return ShellUtils.execValidate(lines -> lines.anyMatch(line -> line.contains(expectedContext)), ALWAYS_VALID, "kubectl", "config", "current-context");
    }
}
