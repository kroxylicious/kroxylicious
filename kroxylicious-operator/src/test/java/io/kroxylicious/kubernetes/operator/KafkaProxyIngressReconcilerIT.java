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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProxyIngressStatusAssert;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyIngressReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconcilerIT.class);

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_BAR_CLUSTERIP_INGRESS = "bar-cluster-ip";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler(TestFiles.INSTALL_MANIFESTS_DIR,
            "*.ClusterRole.kroxylicious-operator-watched.yaml");

    @RegisterExtension
    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect so we can use it as an instance field.
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaProxyIngressReconciler(Clock.systemUTC()))
            .withKubernetesClient(rbacHandler.operatorClient())
            .withAdditionalCustomResourceDefinition(KafkaProxy.class)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();
    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testCreateIngressFirst() {
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertResolvedRefsFalse(ingressBar);
        testActor.create(kafkaProxy(PROXY_A));
        assertAllConditionsTrue(ingressBar);
    }

    @Test
    void testCreateProxyFirst() {
        testActor.create(kafkaProxy(PROXY_A));
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertAllConditionsTrue(ingressBar);
    }

    @Test
    void testDeleteProxy() {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertAllConditionsTrue(ingressBar);

        testActor.delete(proxy);
        assertResolvedRefsFalse(ingressBar);
    }

    @Test
    void testSwitchProxy() {
        testActor.create(kafkaProxy(PROXY_A));
        testActor.create(kafkaProxy(PROXY_B));
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_A));
        assertAllConditionsTrue(ingressBar);

        testActor.replace(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, PROXY_B));
        assertAllConditionsTrue(ingressBar);
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, String proxyName) {
        return new KafkaProxyIngressBuilder().withNewMetadata().withName(ingressName).endMetadata()
                .withNewSpec().withNewClusterIP().withProtocol(TCP).endClusterIP().withNewProxyRef().withName(proxyName).endProxyRef().endSpec()
                .build();
    }

    private void assertAllConditionsTrue(KafkaProxyIngress ingressBar) {
        AWAIT.alias("IngressStatusResolvedRefs").untilAsserted(() -> {
            var kpi = testActor.resources(KafkaProxyIngress.class)
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
            var kpi = testActor.resources(KafkaProxyIngress.class)
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
