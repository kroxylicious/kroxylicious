/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.informer;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.processing.event.EventHandler;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SharedInformerEventSourceTest {

    @Mock
    private SharedIndexInformer<Secret> sharedInformer;
    @Mock
    private SecondaryToPrimaryMapper<Secret> secondaryToPrimaryMapper;
    @Mock
    private PrimaryToSecondaryMapper<TestPrimary> primaryToSecondaryMapper;
    private SharedInformerEventSource<Secret, TestPrimary> eventSource;

    @Test
    void shouldAllowAllNamespacesWhenEmptySet() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of());

        Secret secret = secretInNamespace("test-namespace", "test-secret");
        when(secondaryToPrimaryMapper.toPrimaryResourceIDs(secret))
                .thenReturn(Set.of(new ResourceID("primary-1", "test-namespace")));

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onAdd(secret);

        // then - event is propagated (namespace allowed)
        verify(secondaryToPrimaryMapper).toPrimaryResourceIDs(secret);
    }

    @Test
    void shouldAllowResourceInConfiguredNamespace() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("allowed-ns1", "allowed-ns2"));

        Secret secret = secretInNamespace("allowed-ns1", "test-secret");
        when(secondaryToPrimaryMapper.toPrimaryResourceIDs(secret))
                .thenReturn(Set.of(new ResourceID("primary-1", "allowed-ns1")));

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onAdd(secret);

        // then - event is propagated (namespace allowed)
        verify(secondaryToPrimaryMapper).toPrimaryResourceIDs(secret);
    }

    @Test
    void shouldRejectResourceInNonConfiguredNamespace() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("allowed-ns1", "allowed-ns2"));

        Secret secret = secretInNamespace("other-namespace", "test-secret");

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onAdd(secret);

        // then - event is NOT propagated (namespace rejected)
        verify(secondaryToPrimaryMapper, never()).toPrimaryResourceIDs(any());
    }

    @Test
    void shouldPropagateEventWhenNamespaceAllowed() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("allowed-ns"));

        Secret secret = secretInNamespace("allowed-ns", "test-secret");
        when(secondaryToPrimaryMapper.toPrimaryResourceIDs(secret))
                .thenReturn(Set.of(new ResourceID("primary-1", "allowed-ns")));

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onAdd(secret);

        // then
        verify(secondaryToPrimaryMapper).toPrimaryResourceIDs(secret);
    }

    @Test
    void shouldNotPropagateEventWhenNamespaceNotAllowed() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("allowed-ns"));

        Secret secret = secretInNamespace("other-ns", "test-secret");

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onAdd(secret);

        // then
        verify(secondaryToPrimaryMapper, never()).toPrimaryResourceIDs(any());
    }

    @Test
    void shouldFilterListByPredicate() {
        // given
        Secret secret1 = secretInNamespace("ns1", "secret1");
        Secret secret2 = secretInNamespace("ns1", "secret2");
        Secret secret3 = secretInNamespace("ns2", "secret3");

        when(sharedInformer.getStore()).thenReturn(mock(io.fabric8.kubernetes.client.informers.cache.Store.class));
        when(sharedInformer.getStore().list()).thenReturn(List.of(secret1, secret2, secret3));

        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("ns1"));

        Predicate<Secret> predicate = s -> s.getMetadata().getName().equals("secret1");

        // when
        List<Secret> result = eventSource.list(predicate).toList();

        // then
        assertThat(result).containsExactly(secret1);
    }

    @Test
    void shouldFilterListByNamespace() {
        // given
        Secret secret1 = secretInNamespace("ns1", "secret1");
        Secret secret2 = secretInNamespace("ns1", "secret2");
        Secret secret3 = secretInNamespace("ns2", "secret3");

        when(sharedInformer.getStore()).thenReturn(mock(io.fabric8.kubernetes.client.informers.cache.Store.class));
        when(sharedInformer.getStore().list()).thenReturn(List.of(secret1, secret2, secret3));

        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("ns1"));

        // when - list() returns unfiltered stream from informer, namespace filtering happens elsewhere
        List<Secret> result = eventSource.list(s -> true).toList();

        // then - all secrets from informer are returned
        assertThat(result).containsExactlyInAnyOrder(secret1, secret2, secret3);
    }

    @Test
    void shouldReturnKeysFromInformer() {
        // given
        Secret secret1 = secretInNamespace("ns1", "secret1");
        Secret secret2 = secretInNamespace("ns2", "secret2");

        when(sharedInformer.getStore()).thenReturn(mock(io.fabric8.kubernetes.client.informers.cache.Store.class));
        when(sharedInformer.getStore().list()).thenReturn(List.of(secret1, secret2));

        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("ns1"));

        // when
        List<ResourceID> keys = eventSource.keys().toList();

        // then
        assertThat(keys).hasSize(2);
        assertThat(keys).extracting(ResourceID::getName).containsExactlyInAnyOrder("secret1", "secret2");
    }

    @Test
    void shouldGetResourceByResourceID() {
        // given
        Secret secret = secretInNamespace("ns1", "secret1");
        ResourceID resourceID = new ResourceID("secret1", "ns1");
        String key = "ns1/secret1";

        when(sharedInformer.getStore()).thenReturn(mock(io.fabric8.kubernetes.client.informers.cache.Store.class));
        when(sharedInformer.getStore().getByKey(key)).thenReturn(secret);

        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("ns1"));

        // when
        Secret result = eventSource.get(resourceID).orElse(null);

        // then
        assertThat(result).isSameAs(secret);
    }

    @Test
    void shouldHandleUpdateEvent() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("allowed-ns"));

        Secret oldSecret = secretInNamespace("allowed-ns", "test-secret-old");
        Secret newSecret = secretInNamespace("allowed-ns", "test-secret-new");

        when(secondaryToPrimaryMapper.toPrimaryResourceIDs(oldSecret))
                .thenReturn(Set.of(new ResourceID("primary-1", "allowed-ns")));
        when(secondaryToPrimaryMapper.toPrimaryResourceIDs(newSecret))
                .thenReturn(Set.of(new ResourceID("primary-1", "allowed-ns")));

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onUpdate(oldSecret, newSecret);

        // then - called for both old and new resource
        verify(secondaryToPrimaryMapper).toPrimaryResourceIDs(oldSecret);
        verify(secondaryToPrimaryMapper).toPrimaryResourceIDs(newSecret);
    }

    @Test
    void shouldHandleDeleteEvent() {
        // given
        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("allowed-ns"));

        Secret secret = secretInNamespace("allowed-ns", "test-secret");

        when(secondaryToPrimaryMapper.toPrimaryResourceIDs(secret))
                .thenReturn(Set.of(new ResourceID("primary-1", "allowed-ns")));

        eventSource.setEventHandler(mock(EventHandler.class));

        // when
        eventSource.onDelete(secret, false);

        // then
        verify(secondaryToPrimaryMapper).toPrimaryResourceIDs(secret);
    }

    @Test
    void shouldGetSecondaryResourcesUsingPrimaryToSecondaryMapper() {
        // given
        TestPrimary primary = testPrimary("test-ns", "test-primary");

        Secret secret1 = secretInNamespace("test-ns", "secret1");
        Secret secret2 = secretInNamespace("test-ns", "secret2");
        secretInNamespace("test-ns", "secret3");

        when(sharedInformer.getStore()).thenReturn(mock(io.fabric8.kubernetes.client.informers.cache.Store.class));
        when(sharedInformer.getStore().getByKey("test-ns/secret1")).thenReturn(secret1);
        when(sharedInformer.getStore().getByKey("test-ns/secret2")).thenReturn(secret2);
        when(sharedInformer.getStore().getByKey("test-ns/secret3")).thenReturn(null);

        eventSource = new SharedInformerEventSource<>(
                Secret.class,
                "test-source",
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                Set.of("test-ns"));

        when(primaryToSecondaryMapper.toSecondaryResourceIDs(primary))
                .thenReturn(Set.of(
                        new ResourceID("secret1", "test-ns"),
                        new ResourceID("secret2", "test-ns"),
                        new ResourceID("secret3", "test-ns")));

        // when
        Set<Secret> result = eventSource.getSecondaryResources(primary);

        // then - only existing secrets returned (secret3 not in cache)
        assertThat(result).containsExactlyInAnyOrder(secret1, secret2);
        verify(primaryToSecondaryMapper).toSecondaryResourceIDs(primary);
    }

    private Secret secretInNamespace(String namespace, String name) {
        return new SecretBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(name)
                .endMetadata()
                .build();
    }

    private TestPrimary testPrimary(String namespace, String name) {
        return new TestPrimary(new ObjectMetaBuilder()
                .withNamespace(namespace)
                .withName(name)
                .build());
    }

    record TestPrimary(ObjectMeta metadata) implements HasMetadata {
        @Override
        public ObjectMeta getMetadata() {
            return metadata;
        }

        @Override
        public void setMetadata(ObjectMeta metadata) {
            throw new UnsupportedOperationException("TestPrimary is immutable");
        }

        @Override
        public String getApiVersion() {
            return "test.kroxylicious.io/v1";
        }

        @Override
        public void setApiVersion(String version) {
            throw new UnsupportedOperationException("TestPrimary is immutable");
        }

        @Override
        public String getKind() {
            return "TestPrimary";
        }
    }
}
