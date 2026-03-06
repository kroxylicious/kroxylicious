/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxyingress;

import java.time.Clock;
import java.time.Duration;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.operator.LocalKroxyliciousOperatorExtension;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProxyIngressStatusAssert;

import static io.kroxylicious.kubernetes.api.common.Protocol.TCP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyIngressReconcilerIT {

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_BAR_CLUSTERIP_INGRESS = "bar-cluster-ip";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new KafkaProxyIngressReconciler(Clock.systemUTC()))
            .withClusterRoleGlobs("*.ClusterRole.kroxylicious-operator-watched.yaml")
            .build();

    @Test
    void testCreateIngressFirst() {
        KafkaProxyIngress ingressBar = operator.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertResolvedRefsFalse(ingressBar);
        operator.create(kafkaProxy(PROXY_A));
        assertAllConditionsTrue(ingressBar);
    }

    @Test
    void testCreateProxyFirst() {
        operator.create(kafkaProxy(PROXY_A));
        KafkaProxyIngress ingressBar = operator.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertAllConditionsTrue(ingressBar);
    }

    @Test
    void testDeleteProxy() {
        KafkaProxy proxy = operator.create(kafkaProxy(PROXY_A));
        KafkaProxyIngress ingressBar = operator.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertAllConditionsTrue(ingressBar);

        operator.delete(proxy);
        assertResolvedRefsFalse(ingressBar);
    }

    @Test
    void testSwitchProxy() {
        operator.create(kafkaProxy(PROXY_A));
        operator.create(kafkaProxy(PROXY_B));
        KafkaProxyIngress ingressBar = operator.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertAllConditionsTrue(ingressBar);

        operator.replace(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_B));
        assertAllConditionsTrue(ingressBar);
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, String proxyName) {
        return new KafkaProxyIngressBuilder().withNewMetadata().withName(ingressName).endMetadata()
                .withNewSpec().withNewClusterIP().withProtocol(TCP).endClusterIP().withNewProxyRef().withName(proxyName).endProxyRef().endSpec()
                .build();
    }

    private void assertAllConditionsTrue(KafkaProxyIngress ingressBar) {
        AWAIT.alias("IngressStatusResolvedRefs").untilAsserted(() -> {
            var kpi = operator.resources(KafkaProxyIngress.class)
                    .withName(ResourcesUtil.name(ingressBar)).get();
            assertThat(kpi.getStatus()).isNotNull();
            KafkaProxyIngressStatusAssert
                    .assertThat(kpi.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpi)
                    .conditionList()
                    .singleElement()
                    .isResolvedRefsTrue(kpi);
        });
    }

    private void assertResolvedRefsFalse(KafkaProxyIngress cr) {
        AWAIT.alias("IngressStatusResolvedRefs").untilAsserted(() -> {
            var kpi = operator.resources(KafkaProxyIngress.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(kpi.getStatus()).isNotNull();
            KafkaProxyIngressStatusAssert
                    .assertThat(kpi.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpi)
                    .singleCondition()
                    .hasType(Condition.Type.ResolvedRefs)
                    .hasStatus(Condition.Status.FALSE)
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
