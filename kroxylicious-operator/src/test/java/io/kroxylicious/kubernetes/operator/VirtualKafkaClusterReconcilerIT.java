/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses.Protocol;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatusBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.generation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class VirtualKafkaClusterReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconcilerIT.class);

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_BAR = "bar-cluster";
    private static final String INGRESS_D = "ingress-d";
    private static final String INGRESS_E = "ingress-e";
    private static final String SERVICE_H = "service-h";
    private static final String FILTER_K = "service-k";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole*.yaml");

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create()))
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withKubernetesClient(rbacHandler.operatorClient())
            .withAdditionalCustomResourceDefinition(KafkaProxyIngress.class)
            .withAdditionalCustomResourceDefinition(KafkaService.class)
            .withAdditionalCustomResourceDefinition(KafkaProtocolFilter.class)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void shouldResolveWhenClusterCreatedAfterReferents() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        updateStatusObservedGeneration(testActor.create(filter(FILTER_K)));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileProxyInitiallyAbsent() {
        // Given
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        testActor.create(kafkaProxy(PROXY_A));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileServiceInitiallyAbsent() {
        // Given
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        testActor.create(kafkaProxy(PROXY_A));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileIngressInitiallyAbsent() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        VirtualKafkaCluster clusterBar = testActor.create(resource);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileFilterInitiallyAbsent() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(testActor.create(filter(FILTER_K)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhenProxyDeleted() {
        // Given
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        updateStatusObservedGeneration(testActor.create(filter(FILTER_K)));
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));
        assertAllConditionsTrue(clusterBar);

        // When
        testActor.delete((HasMetadata) proxy);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);
    }

    @Test
    void shouldNotResolveWhenFilterDeleted() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        var filter = updateStatusObservedGeneration(testActor.create(filter(FILTER_K)));
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));
        assertAllConditionsTrue(clusterBar);

        // When
        testActor.delete(filter);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);
    }

    @Test
    void shouldNotResolveWhileIngressRefersToOtherProxy() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        testActor.create(kafkaProxy(PROXY_B));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_B))); // not A, which is what the VKC references

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        VirtualKafkaCluster clusterBar = testActor.create(resource);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_TRANSITIVE_REFS_NOT_FOUND);

        // And when
        updateStatusObservedGeneration(testActor.replace(clusterIpIngress(INGRESS_D, PROXY_A)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileTwoIpIngresses() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_D, PROXY_A)));
        updateStatusObservedGeneration(testActor.create(clusterIpIngress(INGRESS_E, PROXY_A)));

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, List.of(INGRESS_D, INGRESS_E), SERVICE_H, null);
        VirtualKafkaCluster clusterBar = testActor.create(resource);

        // Then
        assertClusterAcceptedFalse(clusterBar, ProxyConfigDependentResource.REASON_INVALID);

        // And when
        testActor.replace(cluster(CLUSTER_BAR, PROXY_A, List.of(INGRESS_D), SERVICE_H, null));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldReportIngressClusterIpBootstrap() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        var cluster = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        var ingress = clusterIpIngress(INGRESS_D, PROXY_A);
        updateStatusObservedGeneration(testActor.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bar-cluster-ingress-d-bootstrap.%s.svc.cluster.local:9292", Protocol.TCP);
    }

    @Test
    void shouldReportIngressClusterIpBootstrapWhenIngressInitiallyAbsent() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(testActor.create(kafkaService(SERVICE_H)));
        var cluster = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        var ingress = clusterIpIngress(INGRESS_D, PROXY_A);

        VirtualKafkaCluster clusterBar = testActor.create(cluster);

        AWAIT.alias("ClusterStatusBootstrapNotPresent").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(clusterBar)).get();
            VirtualKafkaClusterStatus status = vkc.getStatus();
            assertThat(status)
                    .isNotNull()
                    .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                    .isEmpty();
        });

        // When
        updateStatusObservedGeneration(testActor.create(ingress));

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bar-cluster-ingress-d-bootstrap.%s.svc.cluster.local:9292", Protocol.TCP);
    }

    private VirtualKafkaCluster cluster(String clusterName, String proxyName, String ingressName, String serviceName, @Nullable String filterName) {
        return cluster(clusterName, proxyName, List.of(ingressName), serviceName, filterName);
    }

    private VirtualKafkaCluster cluster(String clusterName, String proxyName, List<String> ingressNamees, String serviceName, @Nullable String filterName) {
        var ingresses = ingressNamees.stream().map(name -> new IngressesBuilder().withNewIngressRef().withName(name).endIngressRef().build()).toList();
        // @formatter:off
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(proxyName)
                .endProxyRef()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef()
                    .withName(serviceName)
                .endTargetKafkaServiceRef();
        if (filterName != null) {
            // filters are optional
            specBuilder.addNewFilterRef()
                    .withName(filterName)
                .endFilterRef();
        }
        // @formatter:on
        return specBuilder.endSpec().build();
    }

    private void assertClusterResolvedRefsFalse(VirtualKafkaCluster cr, String expectedReason) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList()
                    .singleOfType(Condition.Type.ResolvedRefs)
                    .hasStatus(Condition.Status.FALSE)
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .hasReason(expectedReason);
        });
    }

    private void assertAllConditionsTrue(VirtualKafkaCluster cr) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            ConditionListAssert conditionListAssert = VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList();
            conditionListAssert
                    .singleOfType(Condition.Type.ResolvedRefs)
                    .isResolvedRefsTrue(vkc);
            conditionListAssert
                    .singleOfType(Condition.Type.Accepted)
                    .isAcceptedTrue(vkc);
        });
    }

    private void assertClusterAcceptedFalse(VirtualKafkaCluster cr,
                                            String expectedReason) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList()
                    .singleOfType(Condition.Type.Accepted)
                    .hasStatus(Condition.Status.FALSE)
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .hasReason(expectedReason);
        });
    }

    private void assertClusterIngressStatusPopulated(VirtualKafkaCluster clusterBar, KafkaProxyIngress ingress, String expectedBootstrapServer, Protocol protocol) {
        AWAIT.alias("ClusterIngressStatus").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(clusterBar)).get();
            var status = vkc.getStatus();
            assertThat(status)
                    .isNotNull()
                    .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                    .singleElement()
                    .satisfies(i -> {
                        assertThat(i.getName()).isEqualTo(ResourcesUtil.name(ingress));
                        assertThat(i.getBootstrapServer()).isEqualTo(expectedBootstrapServer.formatted(extension.getNamespace()));
                        assertThat(i.getProtocol()).isEqualTo(protocol);
                    });
        });
    }

    KafkaProxy kafkaProxy(String name) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
        // @formatter:on
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, String proxyName) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(ingressName)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterIP()
                        .withProtocol(TCP)
                    .endClusterIP()
                    .withNewProxyRef()
                        .withName(proxyName)
                    .endProxyRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaService(String name) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .editOrNewSpec()
                .withBootstrapServers("foo.bootstrap:9090")
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaProtocolFilter filter(String name) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .editOrNewSpec()
                .withType("com.example.Filter")
                .withConfigTemplate(Map.of())
                .endSpec()
                .build();
        // @formatter:on
    }

    // the KafkaProxyReconciler only operates on KafkaProtocolFilters that have been reconciled, ie metadata.status == status.observedGeneration
    private KafkaProtocolFilter updateStatusObservedGeneration(KafkaProtocolFilter filter) {
        filter.setStatus(new KafkaProtocolFilterStatusBuilder().withObservedGeneration(generation(filter)).build());
        return testActor.patchStatus(filter);
    }

    // the KafkaProxyReconciler only operates on KafkaServices that have been reconciled, ie metadata.status == status.observedGeneration
    private KafkaService updateStatusObservedGeneration(KafkaService filter) {
        filter.setStatus(new KafkaServiceStatusBuilder().withObservedGeneration(generation(filter)).build());
        return testActor.patchStatus(filter);
    }

    // the KafkaProxyReconciler only operates on KafkaServices that have been reconciled, ie metadata.status == status.observedGeneration
    private KafkaProxyIngress updateStatusObservedGeneration(KafkaProxyIngress ingress) {
        ingress.setStatus(new KafkaProxyIngressStatusBuilder().withObservedGeneration(generation(ingress)).build());
        return testActor.patchStatus(ingress);
    }

}
