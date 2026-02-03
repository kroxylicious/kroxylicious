/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.systemtests.clients.KafkaClient;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.KcatClient;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.TestTags.EXTERNAL_KAFKA_CLIENTS;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * System tests for non-JVM Kafka client (Kcat, Python, KafkaGo) interoperability
 * producing or consuming messages through Kroxylicious
 */
@Tag(EXTERNAL_KAFKA_CLIENTS)
class NonJVMClientsST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonJVMClientsST.class);
    private final String clusterName = "non-jvm-clients-cluster";
    private static final String MESSAGE = "Hello-world";
    private KroxyliciousOperator kroxyliciousOperator;

    /**
     * Provides all combinations of Kafka clients for interoperability testing.
     *
     * @return stream of (producer, consumer) client combinations
     */
    static Stream<Arguments> clientCombinations() {
        KafkaClient strimzi = KafkaClients.strimziTestClient();
        KafkaClient python = KafkaClients.pythonTestClient();
        KafkaClient kaf = KafkaClients.kaf();
        KafkaClient kcat = KafkaClients.kcat();

        return Stream.of(
                // Non-JVM → Non-JVM clients
                Arguments.of(kcat, kcat),
                Arguments.of(python, python),
                Arguments.of(kaf, kaf),

                // Non-JVM → JVM clients
                Arguments.of(kcat, strimzi),
                Arguments.of(python, strimzi),
                Arguments.of(kaf, strimzi),

                // JVM → Non-JVM clients
                Arguments.of(strimzi, kcat),
                Arguments.of(strimzi, python),
                Arguments.of(strimzi, kaf));
    }

    /**
     * Tests producing and consuming messages through Kroxylicious using different client combinations.
     *
     * @param producer the Kafka client implementation for producing messages
     * @param consumer the Kafka client implementation for consuming messages
     * @param namespace the Kubernetes namespace to deploy resources in
     */
    @ParameterizedTest(name = "testProducingClient{0}ConsumingClient{1}")
    @MethodSource("clientCombinations")
    void produceAndConsumeMessages(KafkaClient producer, KafkaClient consumer, String namespace) {
        // Skip Kcat tests on ARM architecture - kcat only provides x86 binaries
        if (producer instanceof KcatClient || consumer instanceof KcatClient) {
            String localArch = Environment.ARCHITECTURE;
            assumeThat(localArch.equalsIgnoreCase(Constants.ARCHITECTURE_X86)
                    || localArch.equalsIgnoreCase(Constants.ARCHITECTURE_AMD64))
                    .as("Kcat only supports x86_64/amd64 architectures, but running on: " + localArch)
                    .isTrue();
        }

        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        Kroxylicious kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName);
        final String bootstrap = kroxylicious.getBootstrap(clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        final int numberOfMessages = 2;

        LOGGER.atInfo().setMessage("When the message '{}' is sent to the topic '{}'")
                .addArgument(MESSAGE).addArgument(topicName).log();
        producer.inNamespace(namespace).produceMessages(topicName, bootstrap, MESSAGE, numberOfMessages);

        LOGGER.atInfo().log("Then the messages are consumed");
        List<ConsumerRecord> result = consumer.inNamespace(namespace)
                .consumeMessages(topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @BeforeAll
    void setupBefore() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.warn("Skipping kafka deployment. It is already deployed!");
        }
        else {
            LOGGER.info("Deploying Kafka in {} namespace", Constants.KAFKA_DEFAULT_NAMESPACE);

            int kafkaReplicas = 3;
            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }
}
