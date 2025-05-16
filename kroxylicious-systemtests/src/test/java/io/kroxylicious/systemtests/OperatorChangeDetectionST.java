/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.k8s.KubeClient;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;

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
    private KroxyliciousOperator kroxyliciousOperator;

    @Test
    void shouldUpdateDeploymentWhenKafkaProxyIngressChanges(String namespace) {
        // Given
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(kafkaClusterName);
        KubeClient kubeClient = kubeClient(namespace);

        String originalChecksum = getInitialChecksum(namespace, kubeClient);

        KafkaProxyIngress kafkaProxyIngress = kubeClient.getClient().resources(KafkaProxyIngress.class).inNamespace(namespace)
                .withName(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).get();

        KafkaProxyIngress updatedIngress = kafkaProxyIngress.edit().editSpec().editClusterIP().withProtocol(ClusterIP.Protocol.TLS).endClusterIP().endSpec().build();

        // When
        KubeResourceManager.get().createOrUpdateResourceWithWait(updatedIngress);
        LOGGER.info("Kafka proxy ingress edited");

        // Then
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            Deployment proxyDeployment = kubeClient.getDeployment(namespace, "simple");
            assertThat(proxyDeployment).isNotNull();
            OperatorAssertions.assertThat(proxyDeployment.getSpec().getTemplate().getMetadata()).hasAnnotationSatisfying("kroxylicious.io/referent-checksum",
                    value -> assertThat(value).isNotEqualTo(originalChecksum));
        });
    }

    @Test
    void shouldUpdateDeploymentWhenVirtualKafkaClusterChanges(String namespace) {
        // Given
        // @formatter:off
        KafkaProtocolFilterBuilder arbitraryFilter = KroxyliciousFilterTemplates.baseFilterDeployment(namespace, "arbitrary-filter")
                .withNewSpec()
                .withType("io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation")
                .withConfigTemplate(Map.of("findValue", "foo", "replacementValue", "bar"))
                .endSpec();
        // @formatter:on
        resourceManager.createOrUpdateResourceWithWait(arbitraryFilter);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters("test-vkc");
        KubeClient kubeClient = kubeClient(namespace);

        String originalChecksum = getInitialChecksum(namespace, kubeClient);

        VirtualKafkaCluster virtualKafkaCluster = kubeClient.getClient().resources(VirtualKafkaCluster.class).inNamespace(namespace)
                .withName("test-vkc").get();

        FilterRef filterRef = new FilterRefBuilder().withName("arbitrary-filter").build();
        VirtualKafkaCluster updatedVirtualCluster = virtualKafkaCluster.edit().editSpec().withFilterRefs(filterRef).endSpec().build();

        // When
        KubeResourceManager.get().createOrUpdateResourceWithWait(updatedVirtualCluster);
        LOGGER.info("virtual cluster edited");

        // Then
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            Deployment proxyDeployment = kubeClient.getDeployment(namespace, "simple");
            assertThat(proxyDeployment).isNotNull();
            OperatorAssertions.assertThat(proxyDeployment.getSpec().getTemplate().getMetadata()).hasAnnotationSatisfying("kroxylicious.io/referent-checksum",
                    value -> assertThat(value).isNotEqualTo(originalChecksum));
        });
    }

    @Test
    void shouldRolloutDeploymentWhenFilterConfigurationChanges(String namespace) {
        // Given
        // @formatter:off
        KafkaProtocolFilterBuilder arbitraryFilter = KroxyliciousFilterTemplates.baseFilterDeployment(namespace, "arbitrary-filter")
                .withNewSpec()
                    .withType("io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation")
                    .withConfigTemplate(Map.of("findValue", "foo", "replacementValue", "bar"))
                .endSpec();
        // @formatter:on
        KubeClient kubeClient = kubeClient(namespace);
        resourceManager.createOrUpdateResourceWithWait(arbitraryFilter);
        kroxylicious.deployPortIdentifiesNodeWithFilters(kafkaClusterName, List.of("arbitrary-filter"));

        String originalChecksum = getInitialChecksum(namespace, kubeClient);

        // @formatter:off
        KafkaProtocolFilterBuilder updatedProtocolFilter = kubeClient.getClient().resources(KafkaProtocolFilter.class)
                .inNamespace(namespace)
                .withName("arbitrary-filter")
                .get()
                .edit()
                    .editSpec()
                    .withConfigTemplate(Map.of("findValue", "foo", "replacementValue", "updated"))
                .endSpec();
        // @formatter:on

        // When
        resourceManager.createOrUpdateResourceWithWait(updatedProtocolFilter);
        LOGGER.info("Kafka proxy filter updated");

        // Then
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            Deployment proxyDeployment = kubeClient.getDeployment(namespace, "simple");
            OperatorAssertions.assertThat(proxyDeployment.getSpec().getTemplate().getMetadata()).hasAnnotationSatisfying("kroxylicious.io/referent-checksum",
                    value -> assertThat(value).isNotEqualTo(originalChecksum));
        });
    }

    @BeforeEach
    void setUp(String namespace) {
        kroxylicious = new Kroxylicious(namespace);
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    @BeforeAll
    void setupBefore() {
        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    private String getInitialChecksum(String namespace, KubeClient kubeClient) {
        AtomicReference<String> checksumFromAnnotation = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(90))
                .untilAsserted(() -> kubeClient.listPods(namespace, "app.kubernetes.io/name", "kroxylicious-proxy"),
                        proxyPods -> {
                            assertThat(proxyPods)
                                    .singleElement()
                                    .extracting(Pod::getMetadata)
                                    .satisfies(podMetadata -> OperatorAssertions.assertThat(podMetadata)
                                            .hasAnnotationSatisfying("kroxylicious.io/referent-checksum", value -> assertThat(value).isNotBlank()));

                            String checksumFromPod = getChecksumFromAnnotation(proxyPods.get(0));
                            Deployment proxyDeployment = kubeClient.getDeployment(namespace, "simple");
                            OperatorAssertions.assertThat(proxyDeployment.getSpec().getTemplate().getMetadata())
                                    .hasAnnotationSatisfying(
                                            "kroxylicious.io/referent-checksum",
                                            value -> assertThat(value).isEqualTo(checksumFromPod));
                            checksumFromAnnotation.set(checksumFromPod);
                        });
        return checksumFromAnnotation.get();
    }

    private String getChecksumFromAnnotation(HasMetadata entity) {
        return KubernetesResourceUtil.getOrCreateAnnotations(entity).get("kroxylicious.io/referent-checksum");
    }
}
