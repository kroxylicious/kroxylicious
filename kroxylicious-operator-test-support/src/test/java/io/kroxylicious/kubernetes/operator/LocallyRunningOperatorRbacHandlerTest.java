/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.junit.AbstractOperatorExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
class LocallyRunningOperatorRbacHandlerTest {

    // Injected by @EnableKubernetesMockClient — also intercepted by all internal
    // OperatorTestUtils.kubeClient() calls made by the handler under test.
    KubernetesClient kubeClient;

    @TempDir
    Path tempDir;

    // ---- constructor validation ----

    @Test
    void shouldThrowWhenGlobsIsEmpty() {
        assertThatThrownBy(() -> new LocallyRunningOperatorRbacHandler(tempDir))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowWhenGlobContainsNull() {
        assertThatThrownBy(() -> new LocallyRunningOperatorRbacHandler(tempDir, (String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowWhenGlobContainsEmptyString() {
        assertThatThrownBy(() -> new LocallyRunningOperatorRbacHandler(tempDir, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowWhenDirectoryDoesNotExist() {
        assertThatThrownBy(() -> new LocallyRunningOperatorRbacHandler("/nonexistent/path/xyz", "*.yaml"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowWhenPathIsNotDirectory() throws IOException {
        Path file = Files.createFile(tempDir.resolve("notadir.txt"));
        assertThatThrownBy(() -> new LocallyRunningOperatorRbacHandler(file, "*.yaml"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---- YAML loading and glob matching ----

    @Test
    void shouldCreateRolesAndBindingsForMatchingFiles() throws Exception {
        Files.writeString(tempDir.resolve("test.ClusterRole.yaml"), clusterRoleYaml("test-role"));

        var handler = new LocallyRunningOperatorRbacHandler(tempDir, this::freshClient, "*.ClusterRole.yaml");
        handler.beforeEach(mock(ExtensionContext.class));

        var roleNames = kubeClient.rbac().clusterRoles().list().getItems().stream()
                .map(r -> r.getMetadata().getName()).toList();
        assertThat(roleNames).contains("test-role");

        var bindingNames = kubeClient.rbac().clusterRoleBindings().list().getItems().stream()
                .map(b -> b.getMetadata().getName()).toList();
        assertThat(bindingNames).anyMatch(name -> name.contains("test-role"));
    }

    @Test
    void shouldNotLoadFilesNotMatchingGlob() throws Exception {
        Files.writeString(tempDir.resolve("test.ClusterRole.yaml"), clusterRoleYaml("matched-role"));
        Files.writeString(tempDir.resolve("other.yaml"), clusterRoleYaml("unmatched-role"));

        var handler = new LocallyRunningOperatorRbacHandler(tempDir, this::freshClient, "*.ClusterRole.yaml");
        handler.beforeEach(mock(ExtensionContext.class));

        var roleNames = kubeClient.rbac().clusterRoles().list().getItems().stream()
                .map(r -> r.getMetadata().getName()).toList();
        assertThat(roleNames).contains("matched-role").doesNotContain("unmatched-role");
    }

    @Test
    void shouldIgnoreNonClusterRoleResourcesInMatchedFiles() throws Exception {
        // A file matching the glob that contains both a ClusterRole and a ClusterRoleBinding
        Files.writeString(tempDir.resolve("test.ClusterRole.yaml"),
                clusterRoleYaml("my-role") + "\n---\n" + clusterRoleBindingYaml());

        var handler = new LocallyRunningOperatorRbacHandler(tempDir, this::freshClient, "*.ClusterRole.yaml");
        handler.beforeEach(mock(ExtensionContext.class));

        // The ClusterRoleBinding from the file should NOT have been created (only handler-generated bindings)
        var bindingNames = kubeClient.rbac().clusterRoleBindings().list().getItems().stream()
                .map(b -> b.getMetadata().getName()).toList();
        assertThat(bindingNames).isNotEmpty().doesNotContain("my-binding");
    }

    // ---- afterEach cleanup ----

    @Test
    void afterEachShouldDeleteCreatedRolesAndBindings() throws Exception {
        Files.writeString(tempDir.resolve("test.ClusterRole.yaml"), clusterRoleYaml("cleanup-role"));

        var handler = new LocallyRunningOperatorRbacHandler(tempDir, this::freshClient, "*.ClusterRole.yaml");
        var context = mock(ExtensionContext.class);
        handler.beforeEach(context);

        assertThat(kubeClient.rbac().clusterRoles().list().getItems().stream()
                .map(r -> r.getMetadata().getName()).toList()).contains("cleanup-role");

        handler.afterEach(context);

        assertThat(kubeClient.rbac().clusterRoles().list().getItems().stream()
                .map(r -> r.getMetadata().getName()).toList()).doesNotContain("cleanup-role");
        assertThat(kubeClient.rbac().clusterRoleBindings().list().getItems()).isEmpty();
    }

    // ---- testActor ----

    private static final String TEST_NS = "test-namespace";

    @Test
    void testActorCreateAddsResourceToNamespace() throws Exception {
        // Given
        var actor = handlerForTestActor().testActor(operatorExtensionInNamespace(TEST_NS));
        var configMap = new ConfigMapBuilder().withNewMetadata().withName("my-map").endMetadata().build();

        // When
        actor.create(configMap);

        // Then
        assertThat(kubeClient.configMaps().inNamespace(TEST_NS).withName("my-map").get()).isNotNull();
    }

    @Test
    void testActorGetReturnsResourceFromNamespace() throws Exception {
        // Given
        kubeClient.configMaps().inNamespace(TEST_NS)
                .resource(new ConfigMapBuilder().withNewMetadata().withName("my-map").endMetadata().build()).create();
        var actor = handlerForTestActor().testActor(operatorExtensionInNamespace(TEST_NS));

        // When
        var result = actor.get(ConfigMap.class, "my-map");

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getMetadata().getName()).isEqualTo("my-map");
    }

    @Test
    void testActorReplaceUpdatesResourceInNamespace() throws Exception {
        // Given
        var created = kubeClient.configMaps().inNamespace(TEST_NS)
                .resource(new ConfigMapBuilder().withNewMetadata().withName("my-map").endMetadata().build()).create();
        var actor = handlerForTestActor().testActor(operatorExtensionInNamespace(TEST_NS));

        // When
        actor.replace(new ConfigMapBuilder(created).addToData("key", "value").build());

        // Then
        assertThat(kubeClient.configMaps().inNamespace(TEST_NS).withName("my-map").get().getData())
                .containsEntry("key", "value");
    }

    @Test
    void testActorDeleteRemovesResourceFromNamespace() throws Exception {
        // Given
        var created = kubeClient.configMaps().inNamespace(TEST_NS)
                .resource(new ConfigMapBuilder().withNewMetadata().withName("my-map").endMetadata().build()).create();
        var actor = handlerForTestActor().testActor(operatorExtensionInNamespace(TEST_NS));

        // When
        boolean deleted = actor.delete(created);

        // Then
        assertThat(deleted).isTrue();
        assertThat(kubeClient.configMaps().inNamespace(TEST_NS).withName("my-map").get()).isNull();
    }

    @Test
    void testActorResourcesListsResourcesInNamespace() throws Exception {
        // Given
        kubeClient.configMaps().inNamespace(TEST_NS)
                .resource(new ConfigMapBuilder().withNewMetadata().withName("my-map").endMetadata().build()).create();
        var actor = handlerForTestActor().testActor(operatorExtensionInNamespace(TEST_NS));

        // When
        var items = actor.resources(ConfigMap.class).list().getItems();

        // Then
        assertThat(items).extracting(cm -> cm.getMetadata().getName()).contains("my-map");
    }

    // ---- afterAll ----

    @Test
    void afterAllClosesTestActorClient() throws Exception {
        // Given: loadClusterRoles (in constructor) gets a real client; testActor() gets the mock
        var mockClient = mock(KubernetesClient.class);
        Files.writeString(tempDir.resolve("role.ClusterRole.yaml"), clusterRoleYaml("test-role"));
        var callCount = new AtomicInteger();
        var handler = new LocallyRunningOperatorRbacHandler(tempDir,
                () -> callCount.getAndIncrement() == 0 ? freshClient() : mockClient,
                "*.ClusterRole.yaml");
        handler.testActor(operatorExtensionInNamespace(TEST_NS));

        // When
        handler.afterAll(mock(ExtensionContext.class));

        // Then
        verify(mockClient).close();
    }

    @Test
    void afterAllIsNoOpIfTestActorWasNeverCalled() throws Exception {
        // Given
        Files.writeString(tempDir.resolve("role.ClusterRole.yaml"), clusterRoleYaml("test-role"));
        var handler = new LocallyRunningOperatorRbacHandler(tempDir, this::freshClient, "*.ClusterRole.yaml");

        // When / Then: must not throw
        Assertions.assertDoesNotThrow(() -> handler.afterAll(mock(ExtensionContext.class)));
    }

    // ---- helpers ----

    /** Returns a fresh client pointing at the same mock server each call, so try-with-resources in the handler doesn't close the shared {@link #kubeClient}. */
    private KubernetesClient freshClient() {
        return new KubernetesClientBuilder().withConfig(kubeClient.getConfiguration()).build();
    }

    private LocallyRunningOperatorRbacHandler handlerForTestActor() throws IOException {
        Files.writeString(tempDir.resolve("role.ClusterRole.yaml"), clusterRoleYaml("test-role"));
        return new LocallyRunningOperatorRbacHandler(tempDir, this::freshClient, "*.ClusterRole.yaml");
    }

    private static AbstractOperatorExtension operatorExtensionInNamespace(String namespace) {
        var ext = mock(AbstractOperatorExtension.class);
        when(ext.getNamespace()).thenReturn(namespace);
        return ext;
    }

    private static String clusterRoleYaml(String name) {
        return """
                apiVersion: rbac.authorization.k8s.io/v1
                kind: ClusterRole
                metadata:
                  name: %s
                rules: []
                """.formatted(name);
    }

    private static String clusterRoleBindingYaml() {
        return """
                apiVersion: rbac.authorization.k8s.io/v1
                kind: ClusterRoleBinding
                metadata:
                  name: my-binding
                roleRef:
                  apiGroup: rbac.authorization.k8s.io
                  kind: ClusterRole
                  name: my-role
                subjects: []
                """;
    }
}
