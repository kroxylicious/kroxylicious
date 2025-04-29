/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.testframe.resources.KubeResourceManager;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.k8s.KubeClient;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * The Kroxylicious system tests.
 */
class OperatorChangeDetectionST extends AbstractST {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorChangeDetectionST.class);
    private static Kroxylicious kroxylicious;
    private final String kafkaClusterName = "my-cluster";
    protected static final String BROKER_NODE_NAME = "kafka";
    private KroxyliciousOperator kroxyliciousOperator;

    @Test
    void shouldRolloutPodsWhenKafkaProxyIngressChanges(String namespace) {
        // Given
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(kafkaClusterName);

        KubeClient kubeClient = kubeClient(namespace);
        List<Pod> proxyPods = kubeClient.listPods(namespace, "app.kubernetes.io/name", "kroxylicious-proxy");

        assumeThat(proxyPods).hasSize(1);
        Pod originalPod = proxyPods.get(0);

        KafkaProxyIngress kafkaProxyIngress = kubeClient.getClient().resources(KafkaProxyIngress.class).inNamespace(namespace)
                .withName(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).get();

        KafkaProxyIngress updatedIngress = kafkaProxyIngress.edit().editSpec().editProxyRef().withName("wibble").endProxyRef().endSpec().build();

        // When
        KubeResourceManager.get().createOrUpdateResourceWithWait(updatedIngress);

        // Then
        Awaitility.await().untilAsserted(() -> {
            List<Pod> currentProxyPods = kubeClient.listPods(namespace, "app.kubernetes.io/name", "kroxylicious-proxy");
            assertThat(currentProxyPods).singleElement().isNotEqualTo(originalPod);
            OperatorAssertions.assertThat(currentProxyPods.get(0))
                    .hasAnnotationSatisfying("kroxylicious.io/referent-checksum", value -> assertThat(value).isNotBlank());
        });
    }

    @BeforeEach
    void setUp(String namespace) {
        kroxylicious = new Kroxylicious(namespace);
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
 //        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, kafkaClusterName);
 //        if (!kafkaPods.isEmpty()) {
//            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
//        }
 //        else {
//            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();
//
//            KafkaBuilder kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, kafkaClusterName, 3);
//
//            resourceManager.createResourceFromBuilderWithWait(
//                    KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka.build(), 3),
//                    kafka);
//        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }
}
