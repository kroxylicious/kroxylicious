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
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests that verify the install manifests are well-formed and complete.
 * Does not test actual deployment (that's in AbstractWebhookInstallKT).
 */
class InstallManifestKT {
    private final KubernetesClient client = new KubernetesClientBuilder().build();

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

    private static Path getFullInstallManifest() {
        String version = WebhookInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-admission-install-" + version + ".yaml");
        assumeThat(manifest)
                .describedAs("Full install manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    private static Path getCrdsOnlyManifest() {
        String version = WebhookInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-admission-crds-" + version + ".yaml");
        assumeThat(manifest)
                .describedAs("CRDs-only manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    private List<HasMetadata> loadAllResources(Path manifestFile) throws IOException {
        try (var is = Files.newInputStream(manifestFile)) {
            return client.load(is).items().stream()
                    .filter(Objects::nonNull)
                    .toList();
        }
    }
}
