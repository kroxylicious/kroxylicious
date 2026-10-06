/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests that verify the install manifests are well-formed and complete.
 * Does not test actual deployment (that's in AbstractInstallKT).
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
                .contains("Namespace", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Deployment", "CustomResourceDefinition");

        long crdCount = resources.stream().filter(r -> "CustomResourceDefinition".equals(r.getKind())).count();
        assertThat(crdCount).as("Should contain CRDs").isGreaterThan(0);
    }

    @Test
    void shouldContainAllExpectedResources_CrdBundle() throws IOException {
        Path manifest = getCrdsOnlyManifest();
        List<HasMetadata> resources = loadAllResources(manifest);

        assertThat(resources)
                .as("CRDs-only manifest should not be empty")
                .isNotEmpty()
                .as("CRDs-only manifest should contain only CustomResourceDefinition resources")
                .allMatch(r -> "CustomResourceDefinition".equals(r.getKind()));
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

    @ParameterizedTest
    @ValueSource(strings = {
            "install/",
            "CustomResourceDefinition",
            "examples/",
            "docs/"
    })
    void zipArchiveShouldContainExpectedContent(String contentPattern) throws IOException {
        Path zipArchive = getZipArchive();
        try (ZipFile zip = new ZipFile(zipArchive.toFile())) {
            assertThat(zip.stream()
                    .map(ZipEntry::getName)
                    .anyMatch(name -> name.contains(contentPattern) || name.startsWith(contentPattern)))
                    .as("Zip archive should contain " + contentPattern)
                    .isTrue();
        }
    }

    @Test
    void singleFileInstallManifestShouldMatchArchiveManifests() throws IOException {
        List<String> singleFile = normalise(loadAllResources(getFullInstallManifest()));
        List<String> perFile = normalise(loadResourcesFromArchiveInstallDir(getZipArchive()));

        assertThat(singleFile)
                .as("Single-file install.yaml should be functionally equivalent to the per-file manifests in the archive install/ directory")
                .containsExactlyInAnyOrderElementsOf(perFile);
    }

    @Test
    void crdsOnlyManifestShouldMatchCrdSubsetOfFullInstall() throws IOException {
        List<String> crdsOnly = normalise(loadAllResources(getCrdsOnlyManifest()));
        List<String> crdSubset = normalise(loadAllResources(getFullInstallManifest()).stream()
                .filter(r -> "CustomResourceDefinition".equals(r.getKind()))
                .toList());

        assertThat(crdsOnly)
                .as("crds.yaml should be functionally equivalent to the CRD subset of install.yaml")
                .containsExactlyInAnyOrderElementsOf(crdSubset);
    }

    @Test
    void examplesArchiveShouldMatchExamplesInFullArchive() throws IOException {
        Map<String, byte[]> standalone = readEntries(getExamplesArchive(), "", "LICENSE");
        Map<String, byte[]> bundled = readEntries(getZipArchive(), "examples/", null);

        assertThat(standalone.keySet())
                .as("Examples archive should contain the same files as the archive examples/ directory")
                .isEqualTo(bundled.keySet());
        standalone.forEach((name, content) -> assertThat(content)
                .as("Content of example file %s should match between archives", name)
                .isEqualTo(bundled.get(name)));
    }

    private static Path getFullInstallManifest() {
        String version = OperatorInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-operator-" + version + "-install.yaml");
        assumeThat(manifest)
                .describedAs("Full install manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    private static Path getCrdsOnlyManifest() {
        String version = OperatorInfo.fromResource().version();
        Path manifest = Path.of("target/kroxylicious-operator-" + version + "-crds.yaml");
        assumeThat(manifest)
                .describedAs("CRDs-only manifest %s must exist", manifest)
                .exists();
        return manifest;
    }

    private static Path getZipArchive() {
        String version = OperatorInfo.fromResource().version();
        Path archive = Path.of("target/kroxylicious-operator-" + version + ".zip");
        assumeThat(archive)
                .describedAs("Zip archive %s must exist", archive)
                .exists();
        return archive;
    }

    private static Path getExamplesArchive() {
        String version = OperatorInfo.fromResource().version();
        Path archive = Path.of("target/kroxylicious-operator-" + version + "-examples.zip");
        assumeThat(archive)
                .describedAs("Examples archive %s must exist", archive)
                .exists();
        return archive;
    }

    private List<HasMetadata> loadAllResources(Path manifestFile) throws IOException {
        try (var is = Files.newInputStream(manifestFile)) {
            return client.load(is).items().stream()
                    .filter(Objects::nonNull)
                    .toList();
        }
    }

    private List<HasMetadata> loadResourcesFromArchiveInstallDir(Path archive) throws IOException {
        List<HasMetadata> resources = new ArrayList<>();
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            var manifests = zip.stream()
                    .filter(entry -> !entry.isDirectory())
                    .filter(entry -> entry.getName().startsWith("install/"))
                    .filter(entry -> entry.getName().endsWith(".yaml") || entry.getName().endsWith(".yml"))
                    .sorted(Comparator.comparing(ZipEntry::getName))
                    .toList();
            for (ZipEntry entry : manifests) {
                try (var is = zip.getInputStream(entry)) {
                    resources.addAll(client.load(is).items().stream().filter(Objects::nonNull).toList());
                }
            }
        }
        return resources;
    }

    // Serialise each resource to canonical YAML so comparisons ignore comments, formatting and file ordering.
    private static List<String> normalise(List<HasMetadata> resources) {
        return resources.stream().map(Serialization::asYaml).toList();
    }

    private static Map<String, byte[]> readEntries(Path archive, String prefix, String exclude) throws IOException {
        Map<String, byte[]> entries = new TreeMap<>();
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            var iterator = zip.entries();
            while (iterator.hasMoreElements()) {
                ZipEntry entry = iterator.nextElement();
                if (entry.isDirectory() || !entry.getName().startsWith(prefix)) {
                    continue;
                }
                String relative = entry.getName().substring(prefix.length());
                if (relative.isEmpty() || relative.equals(exclude)) {
                    continue;
                }
                try (var is = zip.getInputStream(entry)) {
                    entries.put(relative, is.readAllBytes());
                }
            }
        }
        return entries;
    }
}
