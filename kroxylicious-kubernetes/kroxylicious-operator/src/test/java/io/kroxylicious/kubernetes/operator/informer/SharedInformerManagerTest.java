/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.informer;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
class SharedInformerManagerTest {

    KubernetesClient client;

    @Test
    void shouldReturnSameInformerForSameResourceClass() {
        // given
        SharedInformerManager manager = new SharedInformerManager(client, Set.of());

        // when
        SharedIndexInformer<Secret> informer1 = manager.getOrCreateInformer(Secret.class);
        SharedIndexInformer<Secret> informer2 = manager.getOrCreateInformer(Secret.class);

        // then
        assertThat(informer1).isSameAs(informer2);
    }

    @Test
    void shouldReturnDifferentInformersForDifferentResourceClasses() {
        // given
        SharedInformerManager manager = new SharedInformerManager(client, Set.of());

        // when
        SharedIndexInformer<Secret> secretInformer = manager.getOrCreateInformer(Secret.class);
        SharedIndexInformer<ConfigMap> configMapInformer = manager.getOrCreateInformer(ConfigMap.class);

        // then
        assertThat(secretInformer).isNotSameAs(configMapInformer);
    }

    @Test
    void shouldReturnEmptySetWhenNoNamespacesConfigured() {
        // given
        SharedInformerManager manager = new SharedInformerManager(client, Set.of());

        // when
        Set<String> effectiveNamespaces = manager.getEffectiveNamespaces();

        // then
        assertThat(effectiveNamespaces).isEmpty();
    }

    @Test
    void shouldReturnConfiguredNamespaces() {
        // given
        Set<String> namespaces = Set.of("namespace1", "namespace2");
        SharedInformerManager manager = new SharedInformerManager(client, namespaces);

        // when
        Set<String> effectiveNamespaces = manager.getEffectiveNamespaces();

        // then
        assertThat(effectiveNamespaces).containsExactlyInAnyOrderElementsOf(namespaces);
    }

    @Test
    void shouldCreateInformerForAllNamespacesWhenEmptySet() {
        // given
        SharedInformerManager manager = new SharedInformerManager(client, Set.of());

        // when
        SharedIndexInformer<Secret> informer = manager.getOrCreateInformer(Secret.class);

        // then
        assertThat(informer).isNotNull();
        // Informer is created with inAnyNamespace() - no exception thrown
    }

    @Test
    void shouldCreateInformerForSingleNamespace() {
        // given
        SharedInformerManager manager = new SharedInformerManager(client, Set.of("test-namespace"));

        // when
        SharedIndexInformer<Secret> informer = manager.getOrCreateInformer(Secret.class);

        // then
        assertThat(informer).isNotNull();
        // Informer is created with inNamespace("test-namespace") - no exception thrown
    }

    @Test
    void shouldCreateInformerForMultipleNamespaces() {
        // given
        SharedInformerManager manager = new SharedInformerManager(client, Set.of("ns1", "ns2", "ns3"));

        // when
        SharedIndexInformer<Secret> informer = manager.getOrCreateInformer(Secret.class);

        // then
        assertThat(informer).isNotNull();
        // Informer is created with inAnyNamespace() - no exception thrown
    }
}
