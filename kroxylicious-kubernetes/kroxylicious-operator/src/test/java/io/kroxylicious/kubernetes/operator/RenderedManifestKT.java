/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.kroxylicious.testing.integration.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests that verify the rendered install manifests are complete and usable.
 */
class RenderedManifestKT extends AbstractInstallKT {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenderedManifestKT.class);

    @Test
    void shouldInstallFromRenderedManifest() {
        assumeThat(testImageAvailable()).isTrue();

        Path manifest = getFullInstallManifest();
        try {
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", manifest.toString())).isTrue();

            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-operator",
                    "--for=jsonpath={.status.readyReplicas}=1", "--timeout=300s", "deployment", "kroxylicious-operator")).isTrue();
            LOGGER.info("Operator deployment became ready from full install manifest");
        }
        finally {
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "delete", "-f", manifest.toString());
        }
    }

    @Test
    void shouldInstallFromCrdsOnlyThenOperator() {
        assumeThat(testImageAvailable()).isTrue();

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

    @Test
    void shouldContainAllExpectedResources() throws IOException {
        Path manifest = getFullInstallManifest();
        List<HasMetadata> resources = loadAllResources(manifest);

        assertThat(resources)
                .as("Full install manifest should contain all resource types")
                .extracting(HasMetadata::getKind)
                .contains("Namespace", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Deployment", "CustomResourceDefinition");

        long crdCount = resources.stream().filter(r -> "CustomResourceDefinition".equals(r.getKind())).count();
        assertThat(crdCount).as("Should contain CRDs").isGreaterThan(0);
    }

    @Test
    void shouldCrdsOnlyContainOnlyCrds() throws IOException {
        Path manifest = getCrdsOnlyManifest();
        List<HasMetadata> resources = loadAllResources(manifest);

        assertThat(resources)
                .as("CRDs-only manifest should contain only CustomResourceDefinition resources")
                .allMatch(r -> "CustomResourceDefinition".equals(r.getKind()));

        assertThat(resources)
                .as("CRDs-only manifest should not be empty")
                .isNotEmpty();
    }

    @Test
    void shouldHaveNoUnsubstitutedVariables() throws IOException {
        Path fullManifest = getFullInstallManifest();
        Path crdsManifest = getCrdsOnlyManifest();

        String fullContent = Files.readString(fullManifest);
        String crdsContent = Files.readString(crdsManifest);

        assertThat(fullContent)
                .as("Full install manifest should not contain unsubstituted variables")
                .doesNotContain("$[");

        assertThat(crdsContent)
                .as("CRDs-only manifest should not contain unsubstituted variables")
                .doesNotContain("$[");

        assertThat(fullContent)
                .as("Full install manifest should contain actual image reference")
                .contains("quay.io/kroxylicious/operator:");
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

    private List<HasMetadata> loadAllResources(Path manifestFile) throws IOException {
        String content = Files.readString(manifestFile);
        return Serialization.unmarshalAsList(content);
    }
}
