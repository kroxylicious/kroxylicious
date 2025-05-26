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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.skodjob.testframe.resources.KubeResourceManager;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
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

import static io.kroxylicious.systemtests.TestTags.OPERATOR;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The Kroxylicious system tests.
 */
@Tag(OPERATOR)
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

        updateIngresProtocol(ClusterIP.Protocol.TLS, kubeClient, namespace); // move to TLS but this is invalid

        // When
        // So move back to TCP and check things get updated.
        updateIngresProtocol(ClusterIP.Protocol.TCP, kubeClient, namespace);
        LOGGER.info("Kafka proxy ingress edited");

        // Then
        assertDeploymentUpdated(namespace, kubeClient, originalChecksum);
    }

    private static void updateIngresProtocol(ClusterIP.Protocol protocol, KubeClient kubeClient, String namespace) {
        KafkaProxyIngress kafkaProxyIngress = kubeClient.getClient().resources(KafkaProxyIngress.class).inNamespace(namespace)
                .withName(Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP).get();
        KafkaProxyIngress updatedIngress = kafkaProxyIngress.edit().editSpec().editClusterIP().withProtocol(protocol).endClusterIP().endSpec().build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(updatedIngress);
    }

    @Test
    void shouldUpdateDeploymentWhenVirtualKafkaClusterChanges(String namespace) {
        // Given
        // @formatter:off
        KafkaProtocolFilterBuilder arbitraryFilter = KroxyliciousFilterTemplates.baseFilterDeployment(namespace, "arbitrary-filter")
                .withNewSpec()
                .withType("io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation")
                .withConfigTemplate(Map.of("findPattern", "foo", "replacementValue", "bar"))
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
        assertDeploymentUpdated(namespace, kubeClient, originalChecksum);
    }

    @Test
    void shouldUpdateWhenFilterConfigurationChanges(String namespace) {
        // Given
        // @formatter:off
        KafkaProtocolFilterBuilder arbitraryFilter = KroxyliciousFilterTemplates.baseFilterDeployment(namespace, "arbitrary-filter")
                .withNewSpec()
                    .withType("io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation")
                    .withConfigTemplate(Map.of("transformation", "Replacing", "transformationConfig",  Map.of("findPattern", "foo", "replacementValue", "bar")))
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
                    .withConfigTemplate(Map.of("transformation", "Replacing", "transformationConfig",  Map.of("findPattern", "foo", "replacementValue", "updated")))
                .endSpec();
        // @formatter:on

        // When
        resourceManager.createOrUpdateResourceWithWait(updatedProtocolFilter);
        LOGGER.info("Kafka proxy filter updated");

        // Then
        assertDeploymentUpdated(namespace, kubeClient, originalChecksum);
    }

    @Test
    void shouldUpdateDeploymentWhenKafkaProxyChanges(String namespace) {
        // Given
        KubeClient kubeClient = kubeClient(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(kafkaClusterName);

        String originalChecksum = getInitialChecksum(namespace, kubeClient);

        KafkaProxy kafkaProxy = kubeClient.getClient().resources(KafkaProxy.class).inNamespace(namespace)
                .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).get();

        // @formatter:off
        KafkaProxyBuilder updatedKafkaProxy = kafkaProxy.edit()
                .editOrNewSpec()
                .withPodTemplate(new PodTemplateSpecBuilder().build())
                .endSpec();

        // @formatter:on

        // When
        resourceManager.createOrUpdateResourceWithWait(updatedKafkaProxy);
        LOGGER.info("Kafka proxy updated");

        // Then
        assertDeploymentUpdated(namespace, kubeClient, originalChecksum);
    }

    @Test
    void shouldUpdateDeploymentWhenSecretChanges(String namespace) {
        // Given
        resourceManager.createOrUpdateResourceWithWait(
                new SecretBuilder().withNewMetadata().withName("kilted-kiwi").withNamespace(namespace).endMetadata().withType("kubernetes.io/tls")
                        .withData(Map.of("tls.crt", "whatever", "tls.key", "whatever")),
                KroxyliciousFilterTemplates.baseFilterDeployment(namespace, "arbitrary-filter")
                        .withNewSpec()
                        .withType("io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation")
                        .withConfigTemplate(Map.of("transformation", "Replacing", "transformationConfig",
                                Map.of("findPattern", "foo", "pathToReplacementValue", "${secret:kilted-kiwi:tls.key}")))
                        .endSpec());

        KubeClient kubeClient = kubeClient(namespace);
        kroxylicious.deployPortIdentifiesNodeWithFilters(kafkaClusterName, List.of("arbitrary-filter"));
        LOGGER.info("Kroxylicious deployed");

        String originalChecksum = getInitialChecksum(namespace, kubeClient);

        Secret existingSecret = kubeClient.getClient().resources(Secret.class).inNamespace(namespace)
                .withName("kilted-kiwi").get();

        // When
        resourceManager.createOrUpdateResourceWithWait(existingSecret.edit().withData(Map.of("tls.crt", "whatever", "tls.key", "unlocked")));
        LOGGER.info("secret: kilted-kiwi updated");

        // Then
        assertDeploymentUpdated(namespace, kubeClient, originalChecksum);
    }

    private static void assertDeploymentUpdated(String namespace, KubeClient kubeClient, String originalChecksum) {
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            Deployment proxyDeployment = kubeClient.getDeployment(namespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME);
            Assertions.assertThat(proxyDeployment).isNotNull();
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
