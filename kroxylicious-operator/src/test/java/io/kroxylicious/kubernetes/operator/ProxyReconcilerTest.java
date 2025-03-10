/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedDependentResourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;
import io.kroxylicious.kubernetes.operator.assertj.AssertFactory;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProxyReconcilerTest {

    @Mock
    Context<KafkaProxy> context;

    @Mock
    ManagedDependentResourceContext mdrc;

    private AutoCloseable closeable;

    @BeforeEach
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    void successfulInitialReconciliationShouldResultInReadyTrueCondition() {
        // Given
        // @formatter:off
        long generation = 42L;
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotNull();
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.TRUE);
        first.extracting(Conditions::getMessage).isEqualTo("");
        first.extracting(Conditions::getReason).isEqualTo("");
    }

    @Test
    void failedInitialReconciliationShouldResultInReadyTrueCondition() {
        // Given
        // @formatter:off
        long generation = 42L;
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(generation)
                .withName("my-proxy")
                .endMetadata()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).updateErrorStatus(primary, context, new InvalidResourceException("Resource was terrible"));

        // Then
        assertThat(updateControl.isPatch()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .isPresent().get()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotNull();
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.FALSE);
        first.extracting(Conditions::getMessage).isEqualTo("Resource was terrible");
        first.extracting(Conditions::getReason).isEqualTo("InvalidResourceException");
    }

    @Test
    void remainInReadyTrueShouldRetainTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.TRUE)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.TRUE);
        first.extracting(Conditions::getMessage).isEqualTo("");
        first.extracting(Conditions::getReason).isEqualTo("");
    }

    @Test
    void transitionToReadyFalseShouldChangeTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.TRUE)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).updateErrorStatus(primary, context, new InvalidResourceException("Resource was terrible"));

        // Then
        assertThat(updateControl.isPatch()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull().isPresent().get()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.FALSE);
        first.extracting(Conditions::getMessage).isEqualTo("Resource was terrible");
        first.extracting(Conditions::getReason).isEqualTo(InvalidResourceException.class.getSimpleName());
    }

    @Test
    void remainInReadyFalseShouldRetainTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                 .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.FALSE)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).updateErrorStatus(primary, context, new InvalidResourceException("Resource was terrible"));

        // Then
        assertThat(updateControl.isPatch()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull().isPresent().get()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.FALSE);
        first.extracting(Conditions::getMessage).isEqualTo("Resource was terrible");
        first.extracting(Conditions::getReason).isEqualTo(InvalidResourceException.class.getSimpleName());
    }

    @Test
    void transitionToReadyTrueShouldChangeTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.FALSE)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.TRUE);
        first.extracting(Conditions::getMessage).isEqualTo("");
        first.extracting(Conditions::getReason).isEqualTo("");
    }

    @Test
    void transitionToReadyFalseShouldChangeTransitionTime2() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                    .withNamespace("my-ns")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.TRUE)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        doReturn(mdrc).when(context).managedDependentResourceContext();
        doReturn(Set.of(new VirtualKafkaClusterBuilder().withNewMetadata().withName("my-cluster").withNamespace("my-ns").endMetadata().withNewSpec().withNewProxyRef()
                .withName("my-proxy").endProxyRef().endSpec().build())).when(context).getSecondaryResources(VirtualKafkaCluster.class);
        doReturn(Optional.of(Map.of("my-cluster", ClusterCondition.filterNotFound("my-cluster", "MissingFilter")))).when(mdrc).get(
                SharedKafkaProxyContext.CLUSTER_CONDITIONS_KEY,
                Map.class);
        doReturn(new RuntimeDecl(List.of())).when(mdrc).getMandatory(SharedKafkaProxyContext.RUNTIME_DECL_KEY, Map.class);

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource())
                .isNotNull()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.observedGeneration().isEqualTo(generation);
        statusAssert.singleCondition()
                .isReady()
                .hasObservedGeneration(generation)
                .lastTransitionTimeIsEqualTo(time);
        statusAssert.singleCluster()
                .nameIsEqualTo("my-cluster")
                .singleCondition()
                .isAcceptedFalse("Invalid", "Filter \"MissingFilter\" does not exist.")
                .hasObservedGeneration(generation);
        // TODO .lastTransitionTimeIsEqualTo(time);

    }

    @Test
    void proxyToClusterMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> mockOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(mockOperation);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = buildProxy("proxy");
        VirtualKafkaCluster cluster = baseVirtualKafkaClusterBuilder(proxy, "cluster").build();
        VirtualKafkaCluster clusterForAnotherProxy = baseVirtualKafkaClusterBuilder(buildProxy("anotherproxy"), "anothercluster").build();
        when(mockList.getItems()).thenReturn(List.of(cluster, clusterForAnotherProxy));
        PrimaryToSecondaryMapper<HasMetadata> mapper = ProxyReconciler.proxyToClusterMapper(eventSourceContext);
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void proxyToClusterMapper_NoMatchedClusters() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> mockOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(mockOperation);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        VirtualKafkaCluster clusterForAnotherProxy = baseVirtualKafkaClusterBuilder(buildProxy("anotherProxy"), "cluster").build();
        when(mockList.getItems()).thenReturn(List.of(clusterForAnotherProxy));
        PrimaryToSecondaryMapper<HasMetadata> mapper = ProxyReconciler.proxyToClusterMapper(eventSourceContext);
        KafkaProxy proxy = buildProxy("proxy");
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void clusterToProxyMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = buildProxy("proxy");
        when(mockList.getItems()).thenReturn(List.of(proxy));
        SecondaryToPrimaryMapper<VirtualKafkaCluster> mapper = ProxyReconciler.clusterToProxyMapper(eventSourceContext);
        VirtualKafkaCluster cluster = baseVirtualKafkaClusterBuilder(proxy, "cluster").build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(cluster);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void filterToProxyMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = buildProxy("proxy");
        when(mockList.getItems()).thenReturn(List.of(proxy));
        SecondaryToPrimaryMapper<GenericKubernetesResource> mapper = ProxyReconciler.filterToProxy(eventSourceContext);
        String namespace = "test";
        GenericKubernetesResource filter = new GenericKubernetesResourceBuilder().withNewMetadata().withName("filter").withNamespace(namespace).endMetadata().build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
        verify(mockOperation).inNamespace(namespace);
    }

    @Test
    void proxyToFilterMapping() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> mockOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(mockOperation);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        String proxyName = "proxy";
        String proxyNamespace = "test";
        String filterName = "filter";
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName(proxyName).withNamespace(proxyNamespace).endMetadata().build();
        VirtualKafkaCluster clusterForAnotherProxy = baseVirtualKafkaClusterBuilder(proxy, "cluster").editOrNewSpec()
                .addNewFilter().withName(filterName).endFilter().endSpec().build();
        when(mockList.getItems()).thenReturn(List.of(clusterForAnotherProxy));
        PrimaryToSecondaryMapper<KafkaProxy> mapper = ProxyReconciler.proxyToFilters(eventSourceContext);
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).containsExactly(new ResourceID(filterName, proxyNamespace));
        verify(mockOperation).inNamespace(proxyNamespace);
    }

    @Test
    void proxyToKafkaClusterRefMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy = buildProxy("proxy");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = mockVirtualKafkaClusterListOperation(client);

        KubernetesResourceList<KafkaClusterRef> clusterRefListMock = mockKafkaClusterRefListOperation(client);

        KafkaClusterRef kafkaClusterRef = buildKafkaClusterRef("ref");

        when(clusterRefListMock.getItems()).thenReturn(List.of(kafkaClusterRef));

        VirtualKafkaCluster cluster = buildVirtualKafkaCluster(proxy, "cluster", kafkaClusterRef);

        when(clusterListMock.getItems()).thenReturn(List.of(cluster));

        PrimaryToSecondaryMapper<HasMetadata> mapper = ProxyReconciler.proxyToKafkaClusterRefMapper(eventSourceContext);
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(kafkaClusterRef));
    }

    @Test
    void proxyToKafkaClusterRefMapperDistinguishesByProxy() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy1 = buildProxy("proxy1");
        KafkaProxy proxy2 = buildProxy("proxy2");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = mockVirtualKafkaClusterListOperation(client);

        KubernetesResourceList<KafkaClusterRef> clusterRefListMock = mockKafkaClusterRefListOperation(client);

        KafkaClusterRef kafkaClusterRefProxy1 = buildKafkaClusterRef("proxy1ref");
        KafkaClusterRef kafkaClusterRefProxy2 = buildKafkaClusterRef("proxy2ref");

        when(clusterRefListMock.getItems()).thenReturn(List.of(kafkaClusterRefProxy1, kafkaClusterRefProxy2));

        VirtualKafkaCluster clusterProxy1 = buildVirtualKafkaCluster(proxy1, "proxy1cluster", kafkaClusterRefProxy1);
        VirtualKafkaCluster clusterProxy2 = buildVirtualKafkaCluster(proxy2, "proxy2cluster", kafkaClusterRefProxy2);

        when(clusterListMock.getItems()).thenReturn(List.of(clusterProxy1, clusterProxy2));

        PrimaryToSecondaryMapper<HasMetadata> mapper = ProxyReconciler.proxyToKafkaClusterRefMapper(eventSourceContext);

        assertThat(mapper.toSecondaryResourceIDs(proxy1)).containsExactly(ResourceID.fromResource(kafkaClusterRefProxy1));
        assertThat(mapper.toSecondaryResourceIDs(proxy2)).containsExactly(ResourceID.fromResource(kafkaClusterRefProxy2));
    }

    @Test
    void proxyToKafkaClusterRefMapperHandlesProxyWithMultipleRefs() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy1 = buildProxy("proxy1");
        KafkaProxy proxy2 = buildProxy("proxy2");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = mockVirtualKafkaClusterListOperation(client);

        KubernetesResourceList<KafkaClusterRef> clusterRefListMock = mockKafkaClusterRefListOperation(client);

        KafkaClusterRef kafkaClusterRefProxy1 = buildKafkaClusterRef("proxy1ref");

        KafkaClusterRef kafkaClusterRefProxy2a = buildKafkaClusterRef("proxy2refa");
        KafkaClusterRef kafkaClusterRefProxy2b = buildKafkaClusterRef("proxy2refb");

        when(clusterRefListMock.getItems()).thenReturn(List.of(kafkaClusterRefProxy1, kafkaClusterRefProxy2a, kafkaClusterRefProxy2b));

        VirtualKafkaCluster clusterProxy1 = buildVirtualKafkaCluster(proxy1, "proxy1cluster", kafkaClusterRefProxy1);
        VirtualKafkaCluster clusterProxy2a = buildVirtualKafkaCluster(proxy2, "proxy2clustera", kafkaClusterRefProxy2a);
        VirtualKafkaCluster clusterProxy2b = buildVirtualKafkaCluster(proxy2, "proxy2clusterb", kafkaClusterRefProxy2b);

        when(clusterListMock.getItems()).thenReturn(List.of(clusterProxy1, clusterProxy2a, clusterProxy2b));

        PrimaryToSecondaryMapper<HasMetadata> mapper = ProxyReconciler.proxyToKafkaClusterRefMapper(eventSourceContext);

        assertThat(mapper.toSecondaryResourceIDs(proxy1))
                .containsExactly(ResourceID.fromResource(kafkaClusterRefProxy1));

        assertThat(mapper.toSecondaryResourceIDs(proxy2))
                .containsExactly(ResourceID.fromResource(kafkaClusterRefProxy2a), ResourceID.fromResource(kafkaClusterRefProxy2b));
    }

    @Test
    void proxyToKafkaClusterRefMapperIgnoresDanglingKafkaClusterRef() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy = buildProxy("proxy");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = mockVirtualKafkaClusterListOperation(client);
        mockKafkaClusterRefListOperation(client);

        VirtualKafkaCluster cluster = buildVirtualKafkaCluster(proxy, "cluster", buildKafkaClusterRef("dangle"));

        when(clusterListMock.getItems()).thenReturn(List.of(cluster));

        PrimaryToSecondaryMapper<HasMetadata> mapper = ProxyReconciler.proxyToKafkaClusterRefMapper(eventSourceContext);
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void kafkaClusterRefToProxyMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaClusterRef kafkaClusterRef = buildKafkaClusterRef("ref");
        KafkaProxy proxy = buildProxy("proxy");
        VirtualKafkaCluster cluster = buildVirtualKafkaCluster(proxy, "cluster", kafkaClusterRef);

        KubernetesResourceList<KafkaProxy> mockProxyList = mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(cluster));

        SecondaryToPrimaryMapper<KafkaClusterRef> mapper = ProxyReconciler.kafkaClusterRefToProxyMapper(eventSourceContext);

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaClusterRef);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void kafkaClusterRefToProxyMapperHandlesSharedKafkaClusterRef() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaClusterRef kafkaClusterRef = buildKafkaClusterRef("ref");
        KafkaProxy proxy1 = buildProxy("proxy1");
        KafkaProxy proxy2 = buildProxy("proxy2");
        VirtualKafkaCluster proxy1cluster = buildVirtualKafkaCluster(proxy1, "proxy1cluster", kafkaClusterRef);
        VirtualKafkaCluster proxy2cluster = buildVirtualKafkaCluster(proxy2, "proxy2cluster", kafkaClusterRef);

        KubernetesResourceList<KafkaProxy> mockProxyList = mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy1, proxy2));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(proxy1cluster, proxy2cluster));

        SecondaryToPrimaryMapper<KafkaClusterRef> mapper = ProxyReconciler.kafkaClusterRefToProxyMapper(eventSourceContext);

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaClusterRef);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
    }

    @Test
    void kafkaClusterRefToProxyMapperHandlesOrphanKafkaClusterRef() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaClusterRef orphanKafkaClusterRed = buildKafkaClusterRef("orphan");
        KafkaProxy proxy = buildProxy("proxy");
        VirtualKafkaCluster cluster = buildVirtualKafkaCluster(proxy, "cluster", buildKafkaClusterRef("ref"));

        KubernetesResourceList<KafkaProxy> mockProxyList = mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(cluster));

        SecondaryToPrimaryMapper<KafkaClusterRef> mapper = ProxyReconciler.kafkaClusterRefToProxyMapper(eventSourceContext);

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(orphanKafkaClusterRed);
        assertThat(primaryResourceIDs).isEmpty();
    }

    private KafkaProxy buildProxy(String name) {
        return new KafkaProxyBuilder().withNewMetadata().withName(name).endMetadata().build();
    }

    private KafkaClusterRef buildKafkaClusterRef(String name) {
        return new KafkaClusterRefBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .build();
    }

    private VirtualKafkaCluster buildVirtualKafkaCluster(KafkaProxy kafkaProxy, String name, KafkaClusterRef clusterRef) {
        return baseVirtualKafkaClusterBuilder(kafkaProxy, name)
                .editOrNewSpec()
                .withNewTargetCluster()
                .withNewClusterRef()
                .withName(name(clusterRef))
                .endClusterRef()
                .endTargetCluster()
                .endSpec()
                .build();
    }

    private VirtualKafkaClusterBuilder baseVirtualKafkaClusterBuilder(KafkaProxy kafkaProxy, String name) {
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                .withName(name(kafkaProxy))
                .endProxyRef().endSpec();
    }

    private KubernetesResourceList<KafkaProxy> mockKafkaProxyListOperation(KubernetesClient client) {
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);

        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }

    private KubernetesResourceList<VirtualKafkaCluster> mockVirtualKafkaClusterListOperation(KubernetesClient client) {
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> clusterOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(clusterOperation);

        KubernetesResourceList<VirtualKafkaCluster> clusterList = mock();
        when(clusterOperation.list()).thenReturn(clusterList);
        when(clusterOperation.inNamespace(any())).thenReturn(clusterOperation);
        return clusterList;
    }

    private KubernetesResourceList<KafkaClusterRef> mockKafkaClusterRefListOperation(KubernetesClient client) {
        MixedOperation<KafkaClusterRef, KubernetesResourceList<KafkaClusterRef>, Resource<KafkaClusterRef>> clusterRefOperation = mock();
        when(client.resources(KafkaClusterRef.class)).thenReturn(clusterRefOperation);

        KubernetesResourceList<KafkaClusterRef> clusterRefList = mock();
        when(clusterRefOperation.list()).thenReturn(clusterRefList);
        when(clusterRefOperation.inNamespace(any())).thenReturn(clusterRefOperation);
        return clusterRefList;
    }

}
