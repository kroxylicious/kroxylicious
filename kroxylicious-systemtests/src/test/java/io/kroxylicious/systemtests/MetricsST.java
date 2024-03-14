/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;

import io.kroxylicious.systemtests.enums.ComponentType;
import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
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
@ExtendWith(KroxyliciousExtension.class)
class MetricsST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);
    private final String clusterName = "my-cluster";
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final String MESSAGE = "Hello-world";
    private static Kroxylicious kroxylicious;
    private MetricsCollector kroxyliciousCollector;

    @Test
    void kroxyliciousMetricsBeforeSendingMessages() {
        LOGGER.info("Metrics: " + kroxyliciousCollector.getCollectedData().values());
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

        String bootstrap = kroxylicious.getBootstrap();
        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);
        kroxyliciousCollector.collectMetricsFromPods();
        LOGGER.info("Metrics: " + kroxyliciousCollector.getCollectedData().values());
        assertAll(
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_inbound_downstream_messages_total", 1),
                () -> assertMetricValueHigherThan(kroxyliciousCollector, "kroxylicious_inbound_downstream_messages_total", 0),
                () -> assertMetricValueHigherThan(kroxyliciousCollector, "kroxylicious_inbound_downstream_decoded_messages_total", 0));
    }

    @Test
    void kroxyliciousPayloadSize(String namespace) {
        int numberOfMessages = 1;

        String bootstrap = kroxylicious.getBootstrap();
        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 2);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        LOGGER.info("Then the {} messages are consumed", numberOfMessages);
        String result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received: " + result);
        kroxyliciousCollector.collectMetricsFromPods();
        LOGGER.info("Metrics: " + kroxyliciousCollector.getCollectedData().values());
        assertAll(
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_payload_size_bytes_count", 1),
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_payload_size_bytes_sum", 1),
                () -> assertMetricValueCount(kroxyliciousCollector, "kroxylicious_payload_size_bytes_max", 1));

    }

    @BeforeAll
    void setupEnvironment() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.warn("Skipping kafka deployment. It is already deployed!");
            return;
        }
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KAFKA_DEFAULT_NAMESPACE);

        Kafka kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 3).build();

        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka, 3).build(),
                kafka);
    }

    @SuppressWarnings("java:S2925")
    @BeforeEach
    void beforeEach(String namespace) throws InterruptedException {
        final String scraperName = namespace + "-" + Constants.SCRAPER_LABEL_VALUE;
        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(namespace, scraperName).build());
        cluster.setNamespace(namespace);

        LOGGER.info("Sleeping for {} seconds to give operators and operands some time to stable the metrics values before collecting",
                Constants.RECONCILIATION_INTERVAL.toSeconds());
        Thread.sleep(Constants.RECONCILIATION_INTERVAL.toMillis());

        String scraperPodName = kubeClient().listPodsByPrefixInName(namespace, scraperName).get(0).getMetadata().getName();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortPerBrokerPlainWithNoFilters(clusterName, 1);
        kroxyliciousCollector = new MetricsCollector.Builder()
                .withScraperPodName(scraperPodName)
                .withNamespaceName(namespace)
                .withComponentType(ComponentType.KROXYLICIOUS)
                .withComponentName(Constants.KROXY_DEPLOYMENT_NAME)
                .build();
        kroxyliciousCollector.collectMetricsFromPods();
    }
}
