/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.skodjob.testframe.resources.KubeResourceManager;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.k8s.KubeClient;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
    void shouldUpdateDeploymentWhenKafkaProxyIngressChanges(String namespace) {
        // Given
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(kafkaClusterName);
        KubeClient kubeClient = kubeClient(namespace);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<Pod> proxyPods = kubeClient.listPods(namespace, "app.kubernetes.io/name", "kroxylicious-proxy");
            assertThat(proxyPods).singleElement().extracting(Pod::getMetadata).satisfies(podMetadata -> OperatorAssertions.assertThat(podMetadata)
                    .hasAnnotationSatisfying("kroxylicious.io/referent-checksum", value -> assertThat(value).isNotBlank()));
            Deployment proxyDeployment = kubeClient.getDeployment(namespace, "simple");
            OperatorAssertions.assertThat(proxyDeployment.getSpec().getTemplate().getMetadata()).hasAnnotationSatisfying("kroxylicious.io/referent-checksum",
                    value -> assertThat(value).isEqualTo(getChecksumFromAnnotation(proxyPods.get(0))));
        });
        List<Pod> proxyPods = kubeClient.listPods(namespace, "app.kubernetes.io/name", "kroxylicious-proxy");
        String originalChecksum = getChecksumFromAnnotation(proxyPods.get(0));

        KafkaProxyIngress kafkaProxyIngress = kubeClient.getClient().resources(KafkaProxyIngress.class).inNamespace(namespace)
                .withName(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).get();

        KafkaProxyIngress updatedIngress = kafkaProxyIngress.edit().editSpec().editClusterIP().withProtocol(ClusterIP.Protocol.TLS).endClusterIP().endSpec().build();

        // When
        KubeResourceManager.get().createOrUpdateResourceWithWait(updatedIngress);
        LOGGER.info("Kafka proxy ingress edited");

        // Then
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Deployment proxyDeployment = kubeClient.getDeployment(namespace, "simple");
            OperatorAssertions.assertThat(proxyDeployment.getSpec().getTemplate().getMetadata()).hasAnnotationSatisfying("kroxylicious.io/referent-checksum",
                    value -> assertThat(value).isNotEqualTo(originalChecksum));
        });
    }

    private static String getChecksumFromAnnotation(HasMetadata entity) {
        return KubernetesResourceUtil.getOrCreateAnnotations(entity).get("kroxylicious.io/referent-checksum");
    }

    @BeforeEach
    void setUp(String namespace) {
        kroxylicious = new Kroxylicious(namespace);
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }

    @BeforeAll
    void setupBefore() {
        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }
}
