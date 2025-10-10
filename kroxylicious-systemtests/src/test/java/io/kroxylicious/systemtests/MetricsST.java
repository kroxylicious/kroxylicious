/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;
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
import io.kroxylicious.test.tester.SimpleMetric;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static io.kroxylicious.test.tester.SimpleMetricAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * This test suite is designed for testing metrics exposed by kroxylicious.
 */
class MetricsST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsST.class);
    private final String clusterName = "my-cluster";
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final String RECORD_VALUE = "Hello-world";
    private MetricsCollector kroxyliciousCollector;
    private String bootstrap;
    private KroxyliciousOperator kroxyliciousOperator;

    @Test
    void kroxyliciousMessageTotals(String namespace) {
        int numberOfRecords = 1;

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfRecords).addArgument(RECORD_VALUE).addArgument(topicName)
                .log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, RECORD_VALUE, numberOfRecords);
        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfRecords, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        kroxyliciousCollector.collectMetricsFromPods();
        LOGGER.atInfo().setMessage("Metrics: {}").addArgument(kroxyliciousCollector.getCollectedData().values()).log();

        var parsedMetrics = convertToSimpleMetrics(kroxyliciousCollector);

        var produceLabels = Map.of("api_key", ApiKeys.PRODUCE.name(), "node_id", "0");
        var fetchLabels = Map.of("api_key", ApiKeys.FETCH.name(), "node_id", "0");

        assertAll(
                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_client_to_proxy_request_total", produceLabels)
                        .value()
                        .isOne() /* We send one record, so there has to be exactly one produce message */,

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_server_request_total", produceLabels)
                        .value()
                        .isOne(),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_server_to_proxy_response_total", produceLabels)
                        .value()
                        .isOne(),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_client_response_total", produceLabels)
                        .value()
                        .isOne(),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_client_to_proxy_request_total", fetchLabels)
                        .value()
                        .isGreaterThanOrEqualTo(1), /* We expect to consume 1 record, but the client may call fetch more than once */

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_server_request_total", fetchLabels)
                        .value()
                        .isGreaterThanOrEqualTo(1),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_server_to_proxy_response_total", fetchLabels)
                        .value()
                        .isGreaterThanOrEqualTo(1),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_client_response_total", fetchLabels)
                        .value()
                        .isGreaterThanOrEqualTo(1));
    }

    @Test
    void kroxyliciousMessageSize(String namespace) {
        int numberOfMessages = 1;

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(RECORD_VALUE).addArgument(topicName)
                .log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, RECORD_VALUE, numberOfMessages);
        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();
        kroxyliciousCollector.collectMetricsFromPods();
        LOGGER.atInfo().setMessage("Metrics: {}").addArgument(kroxyliciousCollector.getCollectedData().values()).log();

        int recordValueSize = RECORD_VALUE.getBytes(StandardCharsets.UTF_8).length;
        var parsedMetrics = convertToSimpleMetrics(kroxyliciousCollector);

        var produceLabels = Map.of("api_key", ApiKeys.PRODUCE.name(), "node_id", "0");
        var fetchLabels = Map.of("api_key", ApiKeys.FETCH.name(), "node_id", "0");

        assertAll(
                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_client_to_proxy_request_size_bytes_count", produceLabels)
                        .value()
                        .isOne(), /* We send one record, so there has to be exactly one produce message */

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_client_to_proxy_request_size_bytes_sum", produceLabels)
                        .value()
                        .isGreaterThan(recordValueSize),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_server_request_size_bytes_count", produceLabels)
                        .value()
                        .isOne(),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_server_request_size_bytes_sum", produceLabels)
                        .value()
                        .isGreaterThan(recordValueSize),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_server_to_proxy_response_size_bytes_count", fetchLabels)
                        .value()
                        .isGreaterThanOrEqualTo(1), /* We expect to consume 1 record, but the client may call fetch more than once */

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_server_to_proxy_response_size_bytes_sum", fetchLabels)
                        .value()
                        .isGreaterThan(recordValueSize),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_client_response_size_bytes_count", fetchLabels)
                        .value()
                        .isGreaterThanOrEqualTo(1),

                () -> assertThat(parsedMetrics)
                        .withUniqueMetric("kroxylicious_proxy_to_client_response_size_bytes_sum", fetchLabels)
                        .value()
                        .isGreaterThan(recordValueSize)

        );
    }

    @BeforeAll
    void setupEnvironment() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int numberOfBrokers = 1;
            KafkaBuilder kafka = KafkaTemplates.kafkaPersistentWithKRaftAnnotations(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, numberOfBrokers);

            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka.build(), numberOfBrokers),
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
        final String scraperName = namespace + "-" + Constants.SCRAPER_LABEL_VALUE;
        resourceManager.createResourceFromBuilderWithWait(ScraperTemplates.scraperPod(namespace, scraperName));
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

    @NonNull
    private static List<SimpleMetric> convertToSimpleMetrics(MetricsCollector collector) {
        return collector.getCollectedData().values().stream()
                .map(SimpleMetric::parse)
                .flatMap(List::stream).toList();
    }
}
