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
import io.kroxylicious.kubernetes.operator.assertj.KafkaProxyIngressStatusAssert;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyIngressReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconcilerIT.class);

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole.kroxylicious-operator-watched.yaml");

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
        String proxyName = "proxy-a";
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxyName(proxyName).create();
        assertIngressStatusResolvedRefs(ingress, Condition.Status.FALSE);
        testActor.kafkaProxy().withName(proxyName).create();
        assertIngressStatusResolvedRefs(ingress, Condition.Status.TRUE);
    }

    @Test
    void testCreateProxyFirst() {
        KafkaProxy kafkaProxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingressBar = testActor.ingress().withClusterIpForProxy(kafkaProxy).create();
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);
    }

    @Test
    void testDeleteProxy() {
        KafkaProxy proxy = testActor.kafkaProxy().create();
        KafkaProxyIngress ingressBar = testActor.ingress().withClusterIpForProxy(proxy).create();
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);

        testActor.delete(proxy);
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.FALSE);
    }

    @Test
    void testSwitchProxy() {
        KafkaProxy kafkaProxyA = testActor.kafkaProxy().create();
        KafkaProxy kafkaProxyB = testActor.kafkaProxy().create();
        KafkaProxyIngress ingressBar = testActor.ingress().withClusterIpForProxy(kafkaProxyA).create();
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);

        testActor.ingress(name(ingressBar)).withClusterIpForProxy(kafkaProxyB).replace();
        assertIngressStatusResolvedRefs(ingressBar, Condition.Status.TRUE);
    }

    private void assertIngressStatusResolvedRefs(KafkaProxyIngress cr, Condition.Status conditionStatus) {
        AWAIT.alias("IngressStatusResolvedRefs").untilAsserted(() -> {
            var kpi = testActor.resources(KafkaProxyIngress.class)
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

}
