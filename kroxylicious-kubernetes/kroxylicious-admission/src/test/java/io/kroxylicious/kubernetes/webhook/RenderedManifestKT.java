/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
class RenderedManifestKT {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenderedManifestKT.class);
    private static final Predicate<Stream<String>> ALWAYS_VALID = lines -> true;

    @Test
    void shouldInstallFromRenderedManifest() {
        assumeThat(testImageAvailable()).isTrue();

        Path manifest = getFullInstallManifest();
        try {
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", manifest.toString())).isTrue();

            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-webhook",
                    "--for=jsonpath={.status.readyReplicas}=2", "--timeout=300s", "deployment", "kroxylicious-webhook")).isTrue();
            LOGGER.info("Webhook deployment became ready from full install manifest");
        }
        finally {
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "delete", "-f", manifest.toString());
        }
    }

    @Test
    void shouldInstallFromCrdsOnlyThenWebhook() {
        assumeThat(testImageAvailable()).isTrue();

        Path crdsManifest = getCrdsOnlyManifest();
        Path fullManifest = getFullInstallManifest();

        try {
            // Install CRDs first
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", crdsManifest.toString())).isTrue();
            LOGGER.info("Applied CRDs-only manifest");

            // Verify CRDs are established
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "--for=condition=Established",
                    "--timeout=60s", "crd", "proxyconfigs.sidecar.kroxylicious.io")).isTrue();
            LOGGER.info("CRDs became established");

            // Then install full manifest (should work even though CRDs already exist)
            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", fullManifest.toString())).isTrue();

            assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-webhook",
                    "--for=jsonpath={.status.readyReplicas}=2", "--timeout=300s", "deployment", "kroxylicious-webhook")).isTrue();
            LOGGER.info("Webhook deployment became ready after two-stage installation");
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
                .contains("Namespace", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Deployment",
                        "Service", "PodDisruptionBudget", "MutatingWebhookConfiguration", "CustomResourceDefinition");

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
                .contains("quay.io/kroxylicious/webhook:");
    }

    private boolean testImageAvailable() {
        String imageArchive = WebhookInfo.fromResource().imageArchive();
        assumeThat(Path.of(imageArchive))
                .describedAs("Container image archive %s must exist", imageArchive)
                .withFailMessage("Container image archive %s did not exist", imageArchive)
                .exists();
        return true;
    }

    private Path getFullInstallManifest() {
        String version = WebhookInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-admission-install-" + version + ".yaml");
        assumeThat(manifest)
                .describedAs("Full install manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    private Path getCrdsOnlyManifest() {
        String version = WebhookInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-admission-crds-" + version + ".yaml");
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
