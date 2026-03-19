/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
class ClusterUserTest {

    private static final String NAMESPACE = "test-ns";

    KubernetesClient client;

    @BeforeEach
    void createNamespace() {
        client.namespaces().resource(new io.fabric8.kubernetes.api.model.NamespaceBuilder()
                .withNewMetadata().withName(NAMESPACE).endMetadata().build()).create();
    }

    @Test
    void createPersistsResourceInNamespace() {
        var user = new ClusterUser(client, NAMESPACE);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();

        user.create(proxy);

        assertThat(client.resources(KafkaProxy.class).inNamespace(NAMESPACE).withName("my-proxy").get())
                .isNotNull();
    }

    @Test
    void getReturnsResourceByName() {
        var user = new ClusterUser(client, NAMESPACE);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();
        client.resource(proxy).inNamespace(NAMESPACE).create();

        KafkaProxy result = user.get(KafkaProxy.class, "my-proxy");

        assertThat(result).isNotNull();
        assertThat(result.getMetadata().getName()).isEqualTo("my-proxy");
    }

    @Test
    void getMissingResourceReturnsNull() {
        var user = new ClusterUser(client, NAMESPACE);

        assertThat(user.get(KafkaProxy.class, "does-not-exist")).isNull();
    }

    @Test
    void replaceUpdatesExistingResource() {
        var user = new ClusterUser(client, NAMESPACE);
        Secret secret = new SecretBuilder().withNewMetadata().withName("my-secret").endMetadata()
                .addToStringData("key", "original").build();
        client.resource(secret).inNamespace(NAMESPACE).create();

        Secret updated = new SecretBuilder(secret).addToStringData("key", "updated").build();
        user.replace(updated);

        Secret fetched = client.resources(Secret.class).inNamespace(NAMESPACE).withName("my-secret").get();
        assertThat(fetched.getStringData()).containsEntry("key", "updated");
    }

    @Test
    void deleteRemovesResourceFromNamespace() {
        var user = new ClusterUser(client, NAMESPACE);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();
        client.resource(proxy).inNamespace(NAMESPACE).create();

        user.delete(proxy);

        assertThat(client.resources(KafkaProxy.class).inNamespace(NAMESPACE).withName("my-proxy").get())
                .isNull();
    }

    @Test
    void resourcesReturnsNamespaceScopedOperation() {
        var user = new ClusterUser(client, NAMESPACE);
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName("my-proxy").endMetadata().build();
        client.resource(proxy).inNamespace(NAMESPACE).create();

        var items = user.resources(KafkaProxy.class).list().getItems();

        assertThat(items).hasSize(1);
        assertThat(items.get(0).getMetadata().getName()).isEqualTo("my-proxy");
    }
}
