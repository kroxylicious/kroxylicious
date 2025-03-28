/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class VirtualKafkaClusterReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconcilerIT.class);

    private static final String PROXY_A = "proxy-a";
    private static final String CLUSTER_BAR = "bar-cluster";
    private static final String INGRESS_D = "ingress-d";
    private static final String SERVICE_H = "service-h";
    private static final String FILTER_K = "service-k";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole*.yaml");

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC()))
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
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));
        testActor.create(kafkaService(SERVICE_H));
        testActor.create(filter(FILTER_K));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileProxyInitiallyAbsent() {
        // Given
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));
        testActor.create(kafkaService(SERVICE_H));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE, null);

        // And When
        testActor.create(kafkaProxy(PROXY_A));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileServiceInitiallyAbsent() {
        // Given
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));
        testActor.create(kafkaProxy(PROXY_A));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE, null);

        // And When
        testActor.create(kafkaService(SERVICE_H));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileIngressInitiallyAbsent() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        testActor.create(kafkaService(SERVICE_H));

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        VirtualKafkaCluster clusterBar = testActor.create(resource);

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE, null);

        // And When
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileFilterInitiallyAbsent() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));
        testActor.create(kafkaService(SERVICE_H));

        // When
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE, null);

        // And When
        testActor.create(filter(FILTER_K));

        // Then
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhenProxyDeleted() {
        // Given
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));
        testActor.create(kafkaService(SERVICE_H));
        testActor.create(filter(FILTER_K));
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);

        // When
        testActor.delete((HasMetadata) proxy);

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE, null);
    }

    @Test
    void shouldNotResolveWhenFilterDeleted() {
        // Given
        testActor.create(kafkaProxy(PROXY_A));
        testActor.create(clusterIpIngress(INGRESS_D, PROXY_A));
        testActor.create(kafkaService(SERVICE_H));
        var filter = testActor.create(filter(FILTER_K));
        VirtualKafkaCluster clusterBar = testActor.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));
        assertClusterStatusResolvedRefs(clusterBar, null, Condition.Status.TRUE);

        // When
        testActor.delete(filter);

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE, null);
    }

    private VirtualKafkaCluster cluster(String clusterName, String proxyName, String ingressName, String serviceName, @Nullable String filterName) {
        // @formatter:off
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(proxyName)
                .endProxyRef()
                .addNewIngressRef()
                    .withName(ingressName)
                .endIngressRef()
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

    private void assertClusterStatusResolvedRefs(VirtualKafkaCluster cr,
                                                 @Nullable Condition.Status resolvedRefsStatus,
                                                 @Nullable Condition.Status acceptedStatus) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            ConditionListAssert conditionListAssert = VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList();
            if (acceptedStatus == null) {
                conditionListAssert
                        .containsOnlyTypes(Condition.Type.ResolvedRefs)
                        .singleOfType(Condition.Type.ResolvedRefs)
                        .hasStatus(resolvedRefsStatus)
                        .hasObservedGenerationInSyncWithMetadataOf(vkc);
            }
            else {
                conditionListAssert
                        .containsOnlyTypes(Condition.Type.Accepted);
                conditionListAssert
                        .singleOfType(Condition.Type.Accepted)
                        .hasStatus(acceptedStatus)
                        .hasObservedGenerationInSyncWithMetadataOf(vkc);
            }
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

}
