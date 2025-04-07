/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VirtualKafkaClusterReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    private static final VirtualKafkaClusterStatusFactory STATUS_FACTORY = new VirtualKafkaClusterStatusFactory(TEST_CLOCK);

    public static final String PROXY_NAME = "my-proxy";
    public static final VirtualKafkaCluster CLUSTER_NO_FILTERS = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
            .withName("foo")
            .withNamespace("my-namespace")
            .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
            .withNewProxyRef()
            .withName(PROXY_NAME)
            .endProxyRef()
            .addNewIngressRef()
            .withName("my-ingress")
            .endIngressRef()
            .withNewTargetKafkaServiceRef()
            .withName("my-kafka")
            .endTargetKafkaServiceRef()
            .endSpec()
            .build();
    public static final VirtualKafkaCluster CLUSTER_ONE_FILTER = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editSpec()
            .addNewFilterRef()
            .withName("my-filter")
            .endFilterRef()
            .endSpec()
            .build();

    public static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
            .withName(PROXY_NAME)
            .withGeneration(101L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaService SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
            .withName("my-kafka")
            .withGeneration(201L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
            .withName("my-ingress")
            .withGeneration(301L)
            .endMetadata()
            .withNewSpec()
            .withNewProxyRef().withName(PROXY_NAME).endProxyRef()
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_MY_FILTER = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
            .withName("my-filter")
            .withGeneration(401L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    // @formatter:on

    static List<Arguments> shouldSetResolvedRefsToTrueOrFalse() {
        return List.of(
                Arguments.argumentSet("no filter",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.of(buildProxyConfigMapWithPatch(CLUSTER_NO_FILTERS)),
                        Optional.of(SERVICE),
                        Set.of(INGRESS),
                        Set.of(),
                        (BiConsumer<VirtualKafkaCluster, ConditionListAssert>) VirtualKafkaClusterReconcilerTest::assertAllConditionsTrue),
                Arguments.argumentSet("one filter",
                        CLUSTER_ONE_FILTER,
                        Optional.of(PROXY),
                        Optional.of(buildProxyConfigMapWithPatch(CLUSTER_ONE_FILTER)),
                        Optional.of(SERVICE),
                        Set.of(INGRESS),
                        Set.of(FILTER_MY_FILTER),
                        (BiConsumer<VirtualKafkaCluster, ConditionListAssert>) VirtualKafkaClusterReconcilerTest::assertAllConditionsTrue),
                Arguments.argumentSet("one filter with stale configmap",
                        new VirtualKafkaClusterBuilder(CLUSTER_ONE_FILTER).editOrNewStatus().withObservedGeneration(ResourcesUtil.generation(CLUSTER_NO_FILTERS))
                                .endStatus().build(),
                        Optional.of(PROXY),
                        Optional.of(buildProxyConfigMapWithPatch(
                                new VirtualKafkaClusterBuilder(CLUSTER_ONE_FILTER).editMetadata().withGeneration(40L).endMetadata().build())),
                        Optional.of(SERVICE),
                        Set.of(INGRESS),
                        Set.of(FILTER_MY_FILTER),
                        (BiConsumer<VirtualKafkaCluster, ConditionListAssert>) VirtualKafkaClusterReconcilerTest::assertAllConditionsTrue),
                Arguments.argumentSet("proxy not found",
                        CLUSTER_NO_FILTERS,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Set.of(INGRESS),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.proxyRef references kafkaproxy.kroxylicious.io/my-proxy in namespace 'my-namespace'")),
                Arguments.argumentSet("service not found",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.empty(),
                        Set.of(INGRESS),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka in namespace 'my-namespace'")),
                Arguments.argumentSet("ingress refers to a different proxy than virtual cluster",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Set.of(INGRESS.edit().editSpec().withNewProxyRef().withName("not-my-proxy").endProxyRef().endSpec().build()),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND,
                                "a spec.ingressRef had an inconsistent or missing proxyRef kafkaproxy.kroxylicious.io/not-my-proxy in namespace 'my-namespace'")),
                Arguments.argumentSet("service has unresolved refs",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.of(new KafkaServiceBuilder(SERVICE).withNewStatus().addNewCondition().withType(Condition.Type.ResolvedRefs)
                                .withStatus(Condition.Status.FALSE).endCondition().endStatus().build()),
                        Set.of(INGRESS),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka in namespace 'my-namespace'")),
                Arguments.argumentSet("ingress not found",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Set.of(),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.ingressRefs references kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'my-namespace'")),
                Arguments.argumentSet("ingress has unresolved refs",
                        CLUSTER_NO_FILTERS,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Set.of(new KafkaProxyIngressBuilder(INGRESS).withNewStatus().addNewCondition().withType(Condition.Type.ResolvedRefs)
                                .withStatus(Condition.Status.FALSE).endCondition().endStatus().build()),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.ingressRefs references kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'my-namespace'")),
                Arguments.argumentSet("filter not found",
                        CLUSTER_ONE_FILTER,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Set.of(INGRESS),
                        Set.of(),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.filterRefs references kafkaprotocolfilter.filter.kroxylicious.io/my-filter in namespace 'my-namespace'")),
                Arguments.argumentSet("filter has unresolved refs",
                        CLUSTER_ONE_FILTER,
                        Optional.of(PROXY),
                        Optional.empty(),
                        Optional.of(SERVICE),
                        Set.of(INGRESS),
                        Set.of(new KafkaProtocolFilterBuilder(FILTER_MY_FILTER).withNewStatus().addNewCondition().withType(Condition.Type.ResolvedRefs)
                                .withStatus(Condition.Status.FALSE).endCondition().endStatus().build()),
                        assertResolvedRefsFalse(
                                VirtualKafkaClusterReconciler.TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND,
                                "spec.filterRefs references kafkaprotocolfilter.filter.kroxylicious.io/my-filter in namespace 'my-namespace'")));
    }

    @NonNull
    private static BiConsumer<VirtualKafkaCluster, ConditionListAssert> assertResolvedRefsFalse(
                                                                                                String referencedResourcesNotFound,
                                                                                                String message) {
        return (BiConsumer<VirtualKafkaCluster, ConditionListAssert>) (cluster, cl) -> cl.singleOfType(Condition.Type.ResolvedRefs)
                .hasObservedGenerationInSyncWithMetadataOf(cluster)
                .hasLastTransitionTime(TEST_CLOCK.instant())
                .isResolvedRefsFalse(
                        referencedResourcesNotFound,
                        message);
    }

    @NonNull
    private static ConfigMap buildProxyConfigMapWithPatch(VirtualKafkaCluster clusterOneFilter) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterOneFilter.getSpec().getProxyRef().getName())
                .endMetadata()
                .withData(new ProxyConfigData().addStatusPatchForCluster(
                    ResourcesUtil.name(clusterOneFilter),
                    STATUS_FACTORY.newTrueConditionStatusPatch(clusterOneFilter, Condition.Type.ResolvedRefs)).build())
                .build();
        // @formatter:on
    }

    private static void assertAllConditionsTrue(VirtualKafkaCluster cluster, ConditionListAssert cl) {
        cl.singleElement().isResolvedRefsTrue(cluster);
    }

    @ParameterizedTest
    @MethodSource
    void shouldSetResolvedRefsToTrueOrFalse(VirtualKafkaCluster cluster,
                                            Optional<KafkaProxy> existingProxy,
                                            Optional<ConfigMap> existingProxyConfigMap,
                                            Optional<KafkaService> existingService,
                                            Set<KafkaProxyIngress> existingIngresses,
                                            Set<KafkaProtocolFilter> existingFilters,
                                            BiConsumer<VirtualKafkaCluster, ConditionListAssert> asserter) {
        // given
        Clock z = TEST_CLOCK;
        var reconciler = new VirtualKafkaClusterReconciler(z);

        Context<VirtualKafkaCluster> context = mock(Context.class);
        when(context.getSecondaryResource(KafkaProxy.class, VirtualKafkaClusterReconciler.PROXY_EVENT_SOURCE_NAME)).thenReturn(existingProxy);
        when(context.getSecondaryResource(ConfigMap.class, VirtualKafkaClusterReconciler.PROXY_CONFIG_MAP_EVENT_SOURCE_NAME)).thenReturn(existingProxyConfigMap);
        when(context.getSecondaryResource(KafkaService.class, VirtualKafkaClusterReconciler.SERVICES_EVENT_SOURCE_NAME)).thenReturn(existingService);
        when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(existingIngresses);
        when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(existingFilters);

        // when
        var update = reconciler.reconcile(cluster, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        ConditionListAssert conditionAssert = VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(cluster)
                .conditionList();
        asserter.accept(cluster, conditionAssert);

    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK);

        Context<VirtualKafkaCluster> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(CLUSTER_NO_FILTERS, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER_NO_FILTERS)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER_NO_FILTERS)
                .isResolvedRefsUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(TEST_CLOCK.instant());

    }
}
