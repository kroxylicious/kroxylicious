/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class VirtualKafkaClusterReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconcilerIT.class);

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole*.yaml");

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC()))
            .withKubernetesClient(rbacHandler.operatorClient())
            .withAdditionalCustomResourceDefinition(KafkaProxy.class)
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
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(kafkaProxy).create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers("foo.bootstrap:9090").create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();

        // When
        VirtualKafkaCluster clusterBar = testActor.virtualKafkaCluster().withProxyRef(kafkaProxy).withIngresses(ingress).withFilters(filter)
                .withTargetKafkaService(kafkaService).create();

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileProxyInitiallyAbsent() {
        // Given
        String proxy = "proxy-a";
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxyName(proxy).create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers("foo.bootstrap:9090").create();

        // When
        VirtualKafkaCluster clusterBar = testActor.virtualKafkaCluster().withProxyName(proxy).withIngresses(ingress)
                .withTargetKafkaService(kafkaService).create();

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.FALSE);

        // And When
        testActor.kafkaProxy(proxy).create();

        // Then
        assertClusterStatusResolvedRefs(clusterBar, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileServiceInitiallyAbsent() {
        // Given
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(kafkaProxy).create();

        // When
        String serviceName = "service-h";
        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(kafkaProxy).withIngresses(ingress).withTargetKafkaServiceName(serviceName).create();

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.FALSE);

        // And When
        testActor.kafkaService(serviceName).withBootstrapServers("foo.bootstrap:9092").create();

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileIngressInitiallyAbsent() {
        // Given
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers("foo.bootstrap:9090").create();

        // When
        String ingressName = "ingress-d";
        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(kafkaProxy).withTargetKafkaService(kafkaService).withIngressesNamed(ingressName)
                .withTargetKafkaService(kafkaService).create();

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.FALSE);

        // And When
        testActor.ingress(ingressName).withClusterIpForProxy(kafkaProxy).create();

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhileFilterInitiallyAbsent() {
        // Given
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(kafkaProxy).create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers("foo.bootstrap:9090").create();

        // When
        String filterName = "service-k";
        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(kafkaProxy).withTargetKafkaService(kafkaService).withIngresses(ingress)
                .withTargetKafkaService(kafkaService).withFiltersNamed(filterName).create();

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.FALSE);

        // And When
        testActor.protocolFilter(filterName).withArbitraryFilterConfig().create();

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.TRUE);
    }

    @Test
    void shouldNotResolveWhenProxyDeleted() {
        // Given
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(kafkaProxy).create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers("foo.bootstrap:9090").create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();

        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(kafkaProxy).withTargetKafkaService(kafkaService).withIngresses(ingress)
                .withTargetKafkaService(kafkaService).withFilters(filter).create();
        assertClusterStatusResolvedRefs(cluster, Condition.Status.TRUE);

        // When
        testActor.delete(kafkaProxy);

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.FALSE);
    }

    @Test
    void shouldNotResolveWhenFilterDeleted() {
        // Given
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(kafkaProxy).create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers("foo.bootstrap:9090").create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();

        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(kafkaProxy).withTargetKafkaService(kafkaService).withIngresses(ingress)
                .withTargetKafkaService(kafkaService).withFilters(filter).create();
        assertClusterStatusResolvedRefs(cluster, Condition.Status.TRUE);

        // When
        testActor.delete(filter);

        // Then
        assertClusterStatusResolvedRefs(cluster, Condition.Status.FALSE);
    }

    private void assertClusterStatusResolvedRefs(VirtualKafkaCluster cr, Condition.Status conditionStatus) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = testActor.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .singleCondition()
                    .hasType(Condition.Type.ResolvedRefs)
                    .hasStatus(conditionStatus)
                    .hasObservedGenerationInSyncWithMetadataOf(vkc);
        });
    }

}
