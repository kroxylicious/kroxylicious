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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.kubernetes.api.common.Protocol.TCP;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a proxy stack reconciled from {@code KafkaProxy}/{@code KafkaProxyIngress}/{@code VirtualKafkaCluster}
 * resources with names well past Kubernetes' usual length limits still comes up and works: the operator must fall
 * back to {@code NameBuilder}'s hash-and-truncate behaviour for the Deployment/Service/label names it generates,
 * and the resulting proxy must still be reachable by a real Kafka client.
 */
class LongResourceNamesST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(LongResourceNamesST.class);
    private static final String MESSAGE = "Hello-world";

    // The real Strimzi Kafka cluster (and the KafkaService CR naming it) is unrelated to what we
    // changed, and Strimzi's own cluster naming has its own unrelated length limits, so it stays short.
    private final String kafkaClusterName = "longnames-st-kafka";

    // Deliberately long names for the resources NameBuilder now protects. The ingress+cluster combo
    // exceeds the 51-character CEL limit we removed and pushes the generated ClusterIP bootstrap
    // Service name (`{cluster}-{ingress}-bootstrap`) past the 63-character RFC1035 limit, forcing
    // NameBuilder's hash-and-truncate path. The proxy name exceeds 63 characters, forcing truncation
    // of the `app.kubernetes.io/instance` label value.
    private static final String LONG_PROXY_NAME = "a".repeat(70);
    private static final String LONG_INGRESS_NAME = "b".repeat(45);
    private static final String LONG_VKC_NAME = "c".repeat(45);

    private KroxyliciousOperator kroxyliciousOperator;
    private Kroxylicious kroxylicious;

    @BeforeAll
    void setupBefore() {
        KafkaClients.getKafkaClient().preloadImage();
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, kafkaClusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int kafkaReplicas = 1;
            resourceManager.createOrUpdateResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, kafkaClusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, kafkaClusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    @Test
    void produceAndConsumeThroughProxyWithLongResourceNames(String namespace) {
        // Given
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with a KafkaProxy/KafkaProxyIngress/VirtualKafkaCluster using long names")
                .addArgument(Constants.KROXYLICIOUS_NAMESPACE).log();

        // @formatter:off
        KafkaProxy proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(LONG_PROXY_NAME)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .build();

        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(LONG_INGRESS_NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterIP()
                        .withProtocol(TCP)
                    .endClusterIP()
                    .withNewProxyRef()
                        .withName(LONG_PROXY_NAME)
                    .endProxyRef()
                .endSpec()
                .build();

        KafkaService kafkaService = KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(kafkaClusterName).build();

        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(LONG_VKC_NAME)
                .endMetadata()
                .withNewSpec()
                    .withTargetKafkaServiceRef(new KafkaServiceRefBuilder()
                            .withName(kafkaClusterName)
                            .build())
                    .withNewProxyRef()
                        .withName(LONG_PROXY_NAME)
                    .endProxyRef()
                    .addNewIngress()
                        .withNewIngressRef()
                            .withName(LONG_INGRESS_NAME)
                        .endIngressRef()
                    .endIngress()
                .endSpec()
                .build();
        // @formatter:on

        kroxylicious = new KroxyliciousBuilder()
                .withNamespace(Constants.KROXYLICIOUS_NAMESPACE)
                .withKafkaProxy(proxy)
                .withKafkaProxyIngress(ingress)
                .withKafkaService(kafkaService)
                .withVirtualKafkaCluster(cluster)
                .build();

        // When
        kroxylicious.createOrUpdateResources();
        // this polls VirtualKafkaCluster.status until a bootstrap address is present, proving the
        // Deployment/Service were reconciled successfully despite the long names
        String bootstrap = kroxylicious.getBootstrap(Constants.KROXYLICIOUS_NAMESPACE, LONG_VKC_NAME);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.atInfo().setMessage("When a message '{}' is sent to the topic '{}'").addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, 1);

        // Then
        LOGGER.atInfo().setMessage("Then the message is consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, 1, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected message has not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(1)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }
}
