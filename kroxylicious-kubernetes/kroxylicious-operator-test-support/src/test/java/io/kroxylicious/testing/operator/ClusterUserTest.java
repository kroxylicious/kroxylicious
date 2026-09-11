/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// No explicit namespace cleanup needed: each test instance gets a fresh in-memory mock client.
@EnableKubernetesMockClient(crud = true)
class ClusterUserTest {

    private static final String NAMESPACE = "test-ns";

    KubernetesClient client;

    @BeforeEach
    void createNamespace() {
        client.namespaces().resource(new NamespaceBuilder()
                .withNewMetadata().withName(NAMESPACE).endMetadata().build()).create();
    }

    @Test
    void shouldCreateResourceInNamespace() {
        // Given
        var user = new ClusterUser(client, NAMESPACE);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();

        // When
        user.create(proxy);

        // Then
        assertThat(client.resources(KafkaProxy.class).inNamespace(NAMESPACE).withName("my-proxy").get())
                .isNotNull();
    }

    @Test
    void shouldFindResourceInNamespace() {
        // Given
        String otherNamespace = "other-ns";
        client.namespaces().resource(new NamespaceBuilder()
                .withNewMetadata().withName(otherNamespace).endMetadata().build()).create();
        var user = new ClusterUser(client, NAMESPACE);
        client.resource(new KafkaProxyBuilder().withNewMetadata().withName("my-proxy")
                .addToAnnotations("origin", NAMESPACE).endMetadata().build())
                .inNamespace(NAMESPACE).create();
        client.resource(new KafkaProxyBuilder().withNewMetadata().withName("my-proxy")
                .addToAnnotations("origin", otherNamespace).endMetadata().build())
                .inNamespace(otherNamespace).create();

        // When
        KafkaProxy result = user.get(KafkaProxy.class, "my-proxy");

        // Then
        assertThat(result)
                .isNotNull()
                .extracting(r -> r.getMetadata().getAnnotations().get("origin"))
                .isEqualTo(NAMESPACE);
    }

    @Test
    void shouldReturnNullWhenResourceAbsent() {
        // Given
        var user = new ClusterUser(client, NAMESPACE);

        // When
        KafkaProxy result = user.get(KafkaProxy.class, "does-not-exist");

        // Then
        assertThat(result).isNull();
    }

    @Test
    void shouldUpdateResourceInNamespace() {
        // Given
        var user = new ClusterUser(client, NAMESPACE);
        Secret secret = new SecretBuilder().withNewMetadata().withName("my-secret").endMetadata()
                .addToStringData("key", "original").build();
        client.resource(secret).inNamespace(NAMESPACE).create();
        Secret updated = new SecretBuilder(secret).addToStringData("key", "updated").build();

        // When
        user.replace(updated);

        // Then
        Secret fetched = client.resources(Secret.class).inNamespace(NAMESPACE).withName("my-secret").get();
        assertThat(fetched.getStringData()).containsEntry("key", "updated");
    }

    @Test
    void shouldRemoveResourceFromNamespace() {
        // Given
        String otherNamespace = "other-ns";
        client.namespaces().resource(new NamespaceBuilder()
                .withNewMetadata().withName(otherNamespace).endMetadata().build()).create();
        var user = new ClusterUser(client, NAMESPACE);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();
        client.resource(proxy).inNamespace(NAMESPACE).create();
        client.resource(proxy).inNamespace(otherNamespace).create();

        // When
        user.delete(proxy);

        // Then
        assertThat(client.resources(KafkaProxy.class).inNamespace(NAMESPACE).withName("my-proxy").get())
                .isNull();
        assertThat(client.resources(KafkaProxy.class).inNamespace(otherNamespace).withName("my-proxy").get())
                .isNotNull();
    }

    @Test
    void shouldOnlyListResourcesInConfiguredNamespace() {
        // Given
        String otherNamespace = "other-ns";
        client.namespaces().resource(new NamespaceBuilder()
                .withNewMetadata().withName(otherNamespace).endMetadata().build()).create();
        var user = new ClusterUser(client, NAMESPACE);
        client.resource(new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build())
                .inNamespace(NAMESPACE).create();
        client.resource(new KafkaProxyBuilder().withNewMetadata().withName("other-proxy").endMetadata().build())
                .inNamespace(otherNamespace).create();

        // When
        var items = user.resources(KafkaProxy.class).list().getItems();

        // Then
        assertThat(items)
                .singleElement()
                .extracting(item -> item.getMetadata().getName())
                .isEqualTo("my-proxy");
    }

    @Nested
    class ErrorContextWrapping {

        @SuppressWarnings("unchecked")
        @Test
        void shouldIncludeResourceContextInCreateException() {
            // Given
            KubernetesClient mockClient = mock(KubernetesClient.class);
            NamespaceableResource<KafkaProxy> mockResource = mock(NamespaceableResource.class);
            when(mockClient.resource(any(KafkaProxy.class))).thenReturn(mockResource);
            when(mockResource.inNamespace(NAMESPACE)).thenReturn(mockResource);
            when(mockResource.create()).thenThrow(new KubernetesClientException("forbidden"));
            var user = new ClusterUser(mockClient, NAMESPACE);
            KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();

            // When / Then
            assertThatThrownBy(() -> user.create(proxy))
                    .isInstanceOf(KubernetesClientException.class)
                    .hasMessageContaining("ClusterUser")
                    .hasMessageContaining("create")
                    .hasMessageContaining("KafkaProxy")
                    .hasMessageContaining("my-proxy")
                    .hasCauseInstanceOf(KubernetesClientException.class);
        }

        @SuppressWarnings("unchecked")
        @Test
        void shouldIncludeResourceContextInReplaceException() {
            // Given
            KubernetesClient mockClient = mock(KubernetesClient.class);
            NamespaceableResource<KafkaProxy> mockResource = mock(NamespaceableResource.class);
            when(mockClient.resource(any(KafkaProxy.class))).thenReturn(mockResource);
            when(mockResource.inNamespace(NAMESPACE)).thenReturn(mockResource);
            when(mockResource.update()).thenThrow(new KubernetesClientException("conflict"));
            var user = new ClusterUser(mockClient, NAMESPACE);
            KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();

            // When / Then
            assertThatThrownBy(() -> user.replace(proxy))
                    .isInstanceOf(KubernetesClientException.class)
                    .hasMessageContaining("ClusterUser")
                    .hasMessageContaining("replace")
                    .hasMessageContaining("KafkaProxy")
                    .hasMessageContaining("my-proxy")
                    .hasCauseInstanceOf(KubernetesClientException.class);
        }

        @SuppressWarnings("unchecked")
        @Test
        void shouldIncludeResourceContextInDeleteException() {
            // Given
            KubernetesClient mockClient = mock(KubernetesClient.class);
            NamespaceableResource<KafkaProxy> mockResource = mock(NamespaceableResource.class);
            when(mockClient.resource(any(KafkaProxy.class))).thenReturn(mockResource);
            when(mockResource.inNamespace(NAMESPACE)).thenReturn(mockResource);
            when(mockResource.delete()).thenThrow(new KubernetesClientException("not found"));
            var user = new ClusterUser(mockClient, NAMESPACE);
            KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();

            // When / Then
            assertThatThrownBy(() -> user.delete(proxy))
                    .isInstanceOf(KubernetesClientException.class)
                    .hasMessageContaining("ClusterUser")
                    .hasMessageContaining("delete")
                    .hasMessageContaining("KafkaProxy")
                    .hasMessageContaining("my-proxy")
                    .hasCauseInstanceOf(KubernetesClientException.class);
        }

    }
}
