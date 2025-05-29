/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.config.MicrometerDefinitionBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class MetricsIT {

    @TempDir
    private static Path certsDirectory;

    private static final HostPort PORT_IDENTIFIES_BROKER_BOOTSTRAP = new HostPort("localhost", 9092);
    private static final String SNI_IDENTIFIES_BROKER_BASE_ADDRESS = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("sni");

    private static final HostPort SNI_IDENTIFIES_BROKER_BOOTSTRAP = new HostPort("bootstrap." + SNI_IDENTIFIES_BROKER_BASE_ADDRESS, 9192);
    private static final String SNI_IDENTIFIES_BROKER_ADDRESS_PATTERN = "broker-$(nodeId)." + SNI_IDENTIFIES_BROKER_BASE_ADDRESS;

    private KeytoolCertificateGenerator downstreamCertificateGenerator;
    private Path clientTrustStore;

    @BeforeEach
    void beforeEach() throws Exception {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();

    }

    @AfterEach
    void afterEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @Test
    void nonexistentEndpointGives404(KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var notFoundResp = ahc.getFromAdminEndpoint("nonexistent");
            assertThat(notFoundResp.statusCode())
                    .isEqualTo(HttpResponseStatus.NOT_FOUND.code());
        }
    }

    @Test
    void scrapeEndpointExists(KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var ok = ahc.getFromAdminEndpoint("metrics");
            assertThat(ok.statusCode())
                    .isEqualTo(HttpResponseStatus.OK.code());
            assertThat(ok.body())
                    .isNotEmpty();
        }
    }

    @Test
    void knownPrometheusMetricPresent(KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var counterName = getRandomCounterName();
            Metrics.counter(counterName).increment();
            var metrics = ahc.scrapeMetrics();
            assertThat(metrics)
                    .hasSizeGreaterThan(0)
                    .extracting(SimpleMetric::name, SimpleMetric::value)
                    .contains(tuple(counterName, 1.0));
        }
    }

    @Test
    void prometheusMetricFromNamedBinder(KafkaCluster cluster) {
        var config = proxy(cluster)
                .addToMicrometer(
                        new MicrometerDefinitionBuilder(StandardBindersHook.class.getName()).withConfig("binderNames", List.of("JvmGcMetrics")).build())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            assertThat(ahc.scrapeMetrics())
                    .hasSizeGreaterThan(0)
                    .extracting(SimpleMetric::name)
                    .contains("jvm_gc_memory_allocated_bytes_total");
        }
    }

    @Test
    void prometheusMetricsWithCommonTags(KafkaCluster cluster) {
        var config = proxy(cluster)
                .addToMicrometer(new MicrometerDefinitionBuilder(CommonTagsHook.class.getName()).withConfig("commonTags", Map.of("a", "b")).build())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var counterName = getRandomCounterName();
            Metrics.counter(counterName).increment();

            var metrics = ahc.scrapeMetrics();
            assertThat(metrics)
                    .filteredOn("name", counterName)
                    .singleElement()
                    .extracting(SimpleMetric::labels)
                    .isEqualTo(Map.of("a", "b"));
        }
    }

    @Test
    void shouldIncrementDownstreamMessagesOnProduceRequestWithoutFilter(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer()) {
            var metricList = managementClient.scrapeMetrics();
            var inboundDownstreamMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_messages_total", null);
            var inboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_decoded_messages_total", null);

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            var updatedInboundDownstreamMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_messages_total", null);
            var updatedInboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_decoded_messages_total", null);
            assertThat(updatedInboundDownstreamMessagesMetricsValue).isGreaterThan(inboundDownstreamMessagesMetricsValue);
            assertThat(updatedInboundDownstreamDecodedMessagesMetricsValue).isGreaterThan(inboundDownstreamDecodedMessagesMetricsValue);
        }
    }

    @Test
    void shouldIncrementDownstreamMessagesOnProduceRequestWithFilter(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {

        // the downstream messages and decoded messages is not yet differentiated by ApiKey
        final UUID configInstance = UUID.randomUUID();

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder("filter",
                CreateTopicRequest.class.getName()).withConfig("configInstanceId", configInstance).build();

        var config = proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer()) {
            var metricList = managementClient.scrapeMetrics();
            var inboundDownstreamMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_messages_total", null);
            var inboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_decoded_messages_total", null);

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            var updatedInboundDownstreamMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_messages_total", null);
            var updatedInboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_decoded_messages_total", null);
            assertThat(updatedInboundDownstreamMessagesMetricsValue).isGreaterThan(inboundDownstreamMessagesMetricsValue);
            assertThat(updatedInboundDownstreamDecodedMessagesMetricsValue).isGreaterThan(inboundDownstreamDecodedMessagesMetricsValue);
        }
    }

    @Test
    void shouldIncrementConnectionMetrics(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = proxy(cluster)
                .addToFilterDefinitions()
                .addToDefaultFilters()
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient()) {
            var metricsList = managementClient.scrapeMetrics();
            assertMetricsDoesNotExist(metricsList, "kroxylicious_downstream_connections_total", null);
            assertMetricsDoesNotExist(metricsList, "kroxylicious_upstream_connections_total", null);

            // When
            var producer = tester.producer();
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            assertMetricsWithValue(updatedMetricsList, "kroxylicious_downstream_connections_total", null);
            assertMetricsWithValue(updatedMetricsList, "kroxylicious_upstream_connections_total", null);
        }
    }

    static Stream<Arguments> createDownstreamConnectionError() throws Exception {
        var failQuickDescribeCluster = new DescribeClusterOptions().timeoutMs(2_000);
        // Note that the KeytoolCertificateGenerator generates key stores that are PKCS12 format.
        var downstreamCertificateGenerator = new KeytoolCertificateGenerator();
        downstreamCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "localhost", "KI", "RedHat", null, null, "US");
        var clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        downstreamCertificateGenerator.generateTrustStore(downstreamCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        var list = new ArrayList<Arguments>();

        {
            list.add(Arguments.argumentSet("gateway is tcp, client sends malformed kafka",
                    (Supplier<VirtualClusterGateway>) () -> defaultPortIdentifiesNodeGatewayBuilder(PORT_IDENTIFIES_BROKER_BOOTSTRAP)
                            .withName("default")
                            .build(),
                    (Consumer<KroxyliciousTester>) kroxyliciousTester -> {
                        var hostPort = HostPort.parse(kroxyliciousTester.getBootstrapAddress());
                        try (var sock = new Socket(hostPort.host(), hostPort.port());
                                var output = sock.getOutputStream();
                                var input = sock.getInputStream()) {
                            output.write("This ain't Kafka!".getBytes(StandardCharsets.UTF_8));
                            var ignore = input.readAllBytes();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }));
        }

        {

            list.add(Arguments.argumentSet("gateway is tls, client connects using plain",
                    (Supplier<VirtualClusterGateway>) () -> defaultSniHostIdentifiesNodeGatewayBuilder(SNI_IDENTIFIES_BROKER_BOOTSTRAP,
                            SNI_IDENTIFIES_BROKER_ADDRESS_PATTERN)
                            .withName("default")
                            .withNewTls()
                            .withNewKeyStoreKey()
                            .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                            .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                            .endKeyStoreKey()
                            .endTls()

                            .build(),
                    (Consumer<KroxyliciousTester>) kroxyliciousTester -> {
                        try (var admin = kroxyliciousTester.admin(Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"))) {
                            assertThat(admin.describeCluster(failQuickDescribeCluster).clusterId())
                                    .failsWithin(Duration.ofSeconds(5))
                                    .withThrowableThat()
                                    .withCauseInstanceOf(TimeoutException.class)
                                    .havingCause()
                                    .withMessageStartingWith("Timed out waiting for a node assignment.");
                        }
                    }));
        }

        {
            var unrecognisedSni = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("unrecognised");

            list.add(Arguments.argumentSet("gateway is tls/sni, unrecognised SNI name",
                    (Supplier<VirtualClusterGateway>) () -> defaultSniHostIdentifiesNodeGatewayBuilder(SNI_IDENTIFIES_BROKER_BOOTSTRAP,
                            SNI_IDENTIFIES_BROKER_ADDRESS_PATTERN)
                            .withName("default")
                            .withNewTls()
                            .withNewKeyStoreKey()
                            .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                            .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                            .endKeyStoreKey()
                            .endTls()
                            .build(),
                    (Consumer<KroxyliciousTester>) kroxyliciousTester -> {
                        var hostPort = HostPort.parse(kroxyliciousTester.getBootstrapAddress());
                        var unrecognisedHostPort = unrecognisedSni + ":" + hostPort.port();

                        try (var admin = kroxyliciousTester.admin(
                                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, unrecognisedHostPort,
                                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
                            assertThat(admin.describeCluster(failQuickDescribeCluster).clusterId())
                                    .failsWithin(Duration.ofSeconds(5))
                                    .withThrowableThat()
                                    .withCauseInstanceOf(TimeoutException.class)
                                    .havingCause()
                                    .withMessageStartingWith("Timed out waiting for a node assignment.");
                        }
                    }));
        }
        return list.stream();

    }

    @ParameterizedTest
    @MethodSource(value = "createDownstreamConnectionError")
    void shouldIncrementDownstreamErrorOnConnectionError(Supplier<VirtualClusterGateway> gatewaySupplier, Consumer<KroxyliciousTester> causeConnectionError,
                                                         KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement()
                .editFirstVirtualCluster()
                .withGateways(List.of(gatewaySupplier.get()))
                .endVirtualCluster();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient()) {
            var metricsList = managementClient.scrapeMetrics();
            assertMetricsDoesNotExist(metricsList, "kroxylicious_downstream_error_total", null);

            // When
            causeConnectionError.accept(tester);

            // Then
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> {
                        var updatedMetricsList = managementClient.scrapeMetrics();
                        assertMetricsWithValue(updatedMetricsList, "kroxylicious_downstream_errors_total", null);
                    });
        }
    }

    @Test
    void shouldIncrementPayloadSizeBytesMetricsOnProduceRequest(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = proxy(cluster)
                .addToFilterDefinitions()
                .addToDefaultFilters()
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer();) {
            var metricList = managementClient.scrapeMetrics();
            assertMetricsDoesNotExist(metricList, "kroxylicious_payload_size_bytes_count", ApiKeys.PRODUCE);
            assertMetricsDoesNotExist(metricList, "kroxylicious_payload_size_bytes_sum", ApiKeys.PRODUCE);

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricList = managementClient.scrapeMetrics();
            assertMetricsWithValue(updatedMetricList, "kroxylicious_payload_size_bytes_count", ApiKeys.PRODUCE);
            assertMetricsWithValue(updatedMetricList, "kroxylicious_payload_size_bytes_sum", ApiKeys.PRODUCE);
        }
    }

    void assertMetricsDoesNotExist(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        assertThat(metricList)
                .hasSizeGreaterThan(0)
                .noneSatisfy(simpleMetric -> {
                    assertThat(simpleMetric.name()).isEqualTo(metricsName);
                    if (apiKey != null) {
                        assertThat(simpleMetric.labels()).containsValue(apiKey.toString());
                    }
                });
    }

    void assertMetricsWithValue(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        assertThat(metricList)
                .hasSizeGreaterThan(0)
                .anySatisfy(simpleMetric -> {
                    assertThat(simpleMetric.name()).isEqualTo(metricsName);
                    if (apiKey != null) {
                        assertThat(simpleMetric.labels()).containsValue(apiKey.toString());
                    }
                    assertThat(simpleMetric.value()).isGreaterThan(0);
                });
    }

    double getMetricsValue(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        if (apiKey != null) {
            return metricList.stream().filter(simpleMetric -> simpleMetric.name().equals(metricsName)
                    && simpleMetric.labels().containsValue(apiKey.toString())).findFirst().get().value();
        }
        else {
            return metricList.stream().filter(simpleMetric -> simpleMetric.name().equals(metricsName)).findFirst().get().value();
        }
    }

    @NonNull
    private String getRandomCounterName() {
        return "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
    }
}
