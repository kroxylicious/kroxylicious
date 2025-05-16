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

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;

import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.enums.ComponentType;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.metrics.MetricsCollector;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.metrics.ScraperTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static io.kroxylicious.systemtests.utils.MetricsUtils.assertMetricValue;
import static io.kroxylicious.systemtests.utils.MetricsUtils.assertMetricValueCount;
import static io.kroxylicious.systemtests.utils.MetricsUtils.assertMetricValueHigherThan;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * This test suite is designed for testing metrics exposed by kroxylicious.
 */
class MetricsST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsST.class);
    private final String clusterName = "my-cluster";
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final String MESSAGE = "Hello-world";
    private MetricsCollector kroxyliciousCollector;
    private String bootstrap;
    private KroxyliciousOperator kroxyliciousOperator;

    @Test
    void kroxyliciousMetricsBeforeSendingMessages() {
        LOGGER.atInfo().setMessage("Metrics: {}").addArgument(kroxyliciousCollector.getCollectedData().values()).log();
        assertAll("Checking the presence of the metrics",
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_inbound_downstream_messages_total", 1),
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_inbound_downstream_decoded_messages_total", 1));
        assertAll("Checking the value of the metrics",
                () -> assertMetricValue(kroxyliciousCollector, "kroxylicious_inbound_downstream_messages_total", 0),
                () -> assertMetricValue(kroxyliciousCollector, "kroxylicious_inbound_downstream_decoded_messages_total", 0));
    }

    @Test
    void kroxyliciousDownstreamMessages(String namespace) {
        int numberOfMessages = 1;

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        kroxyliciousCollector.collectMetricsFromPods();
        LOGGER.atInfo().setMessage("Metrics: {}").addArgument(kroxyliciousCollector.getCollectedData().values()).log();
        assertAll(
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_inbound_downstream_messages_total", 1),
                () -> assertMetricValueHigherThan(kroxyliciousCollector, "kroxylicious_inbound_downstream_messages_total", 0),
                () -> assertMetricValueHigherThan(kroxyliciousCollector, "kroxylicious_inbound_downstream_decoded_messages_total", 0));
    }

    @Test
    void kroxyliciousPayloadSize(String namespace) {
        int numberOfMessages = 1;

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        kroxyliciousCollector.collectMetricsFromPods();
        LOGGER.atInfo().setMessage("Metrics: {}").addArgument(kroxyliciousCollector.getCollectedData().values()).log();
        assertAll(
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_payload_size_bytes_count", 1),
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_payload_size_bytes_sum", 1),
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_payload_size_bytes_max", 1));

    }

    @BeforeAll
    void setupEnvironment() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            KafkaBuilder kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 3);

            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka.build(), 3),
                    kafka);
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }

    @SuppressWarnings("java:S2925")
    @BeforeEach
    void beforeEach(String namespace) throws InterruptedException {
        final String scraperName = Constants.SCRAPER_LABEL_VALUE;

        ScraperTemplates.deployPortIdentifiesNodeWithNoFilters(namespace, scraperName);
//        resourceManager.createResourceFromBuilderWithWait(ScraperTemplates.scraperPod(namespace, scraperName));
        cluster.setNamespace(namespace);

        LOGGER.atInfo().setMessage("Sleeping for {} seconds to give operators and operands some time to stabilize before collecting metrics.")
                .addArgument(Constants.RECONCILIATION_INTERVAL.toSeconds()).log();
        Thread.sleep(Constants.RECONCILIATION_INTERVAL.toMillis());

        String scraperPodName = kubeClient().listPodsByPrefixInName(namespace, scraperName).get(0).getMetadata().getName();
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();

        Kroxylicious kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithNoFilters(clusterName);
        bootstrap = kroxylicious.getBootstrap(clusterName);
        kroxyliciousCollector = new MetricsCollector.Builder()
                .withScraperPodName(scraperPodName)
                .withNamespaceName(namespace)
                .withComponentType(ComponentType.KROXYLICIOUS)
                .withComponentName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
                .build();
        kroxyliciousCollector.collectMetricsFromPods();
    }
}
