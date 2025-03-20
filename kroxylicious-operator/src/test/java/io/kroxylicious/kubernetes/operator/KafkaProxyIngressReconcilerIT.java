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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProxyIngressStatusAssert;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyIngressReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconcilerIT.class);

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_BAR_CLUSTERIP_INGRESS = "bar-cluster-ip";

    private KubernetesClient client;
    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @BeforeEach
    void beforeEach() {
        client = OperatorTestUtils.kubeClient();
    }

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaProxyIngressReconciler(Clock.systemUTC()))
            .withKubernetesClient(client)
            .withAdditionalCustomResourceDefinition(KafkaProxy.class)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testCreateIngressFirst() {
        KafkaProxyIngress ingressBar = extension.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.FALSE);
        extension.create(kafkaProxy(PROXY_A));
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);
    }

    @Test
    void testCreateProxyFirst() {
        extension.create(kafkaProxy(PROXY_A));
        KafkaProxyIngress ingressBar = extension.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);
    }

    @Test
    void testDeleteProxy() {
        KafkaProxy proxy = extension.create(kafkaProxy(PROXY_A));
        KafkaProxyIngress ingressBar = extension.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);

        extension.delete(proxy);
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.FALSE);
    }

    @Test
    void testSwitchProxy() {
        extension.create(kafkaProxy(PROXY_A));
        extension.create(kafkaProxy(PROXY_B));
        KafkaProxyIngress ingressBar = extension.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);

        extension.replace(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_B));
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, String proxyName) {
        return new KafkaProxyIngressBuilder().withNewMetadata().withName(ingressName).endMetadata()
                .withNewSpec().withNewClusterIP().withProtocol(TCP).endClusterIP().withNewProxyRef().withName(proxyName).endProxyRef().endSpec()
                .build();
    }

    private void assertIngressStatusResolvedRefs(KafkaProxyIngress cr, Condition.Status conditionStatus) {
        AWAIT.alias("IngressStatusResolvedRefs").untilAsserted(() -> {
            var kpi = extension.resources(KafkaProxyIngress.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(kpi.getStatus()).isNotNull();
            KafkaProxyIngressStatusAssert
                    .assertThat(kpi.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpi)
                    .singleCondition()
                    .hasType(Condition.Type.ResolvedRefs)
                    .hasStatus(conditionStatus)
                    .hasObservedGenerationInSyncWithMetadataOf(kpi);
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

}
