/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeListenerBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(KafkaClusterExtension.class)
class KroxyliciousTestersTest {

    public static final String TOPIC = "example";

    @Test
    void testAdminMethods(KafkaCluster cluster) throws Exception {
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            assertNotNull(tester.admin().describeCluster().clusterId().get(10, TimeUnit.SECONDS));
            assertNotNull(tester.admin(Map.of()).describeCluster().clusterId().get(10, TimeUnit.SECONDS));
            assertNotNull(tester.admin(DEFAULT_VIRTUAL_CLUSTER, Map.of()).describeCluster().clusterId().get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void shouldReturnDifferentInstancesOfAdmin(KafkaCluster cluster) {
        // Given
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            // Then
            assertDistinctInstanceOf(tester::admin);
            assertDistinctInstanceOf(() -> tester.admin(DEFAULT_VIRTUAL_CLUSTER));
            assertDistinctInstanceOf(() -> tester.admin(Map.of()));
            assertDistinctInstanceOf(() -> tester.admin(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
        }
    }

    @Test
    void shouldReturnClosableAdmin(KafkaCluster cluster) {
        // Given
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            // Then
            assertClientIsInstanceOf(CloseableAdmin.class, tester::admin);
            assertClientIsInstanceOf(CloseableAdmin.class, () -> tester.admin(DEFAULT_VIRTUAL_CLUSTER));
            assertClientIsInstanceOf(CloseableAdmin.class, () -> tester.admin(Map.of()));
            assertClientIsInstanceOf(CloseableAdmin.class, () -> tester.admin(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
        }
    }

    @Test
    void shouldReturnDifferentInstancesOfProducer(KafkaCluster cluster) {
        // Given
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            // Then
            assertDistinctInstanceOf(tester::producer);
            assertDistinctInstanceOf(() -> tester.producer(DEFAULT_VIRTUAL_CLUSTER));
            assertDistinctInstanceOf(() -> tester.producer(Map.of()));
            assertDistinctInstanceOf(() -> tester.producer(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
            assertDistinctInstanceOf(() -> tester.producer(Serdes.String(), Serdes.String(), Map.of()));
            assertDistinctInstanceOf(() -> tester.producer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), Map.of()));
        }
    }

    @Test
    void shouldReturnClosableProducer(KafkaCluster cluster) {
        // Given
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            // Then
            assertClientIsInstanceOf(CloseableProducer.class, tester::producer);
            assertClientIsInstanceOf(CloseableProducer.class, () -> tester.producer(DEFAULT_VIRTUAL_CLUSTER));
            assertClientIsInstanceOf(CloseableProducer.class, () -> tester.producer(Map.of()));
            assertClientIsInstanceOf(CloseableProducer.class, () -> tester.producer(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
            assertClientIsInstanceOf(CloseableProducer.class, () -> tester.producer(Serdes.String(), Serdes.String(), Map.of()));
            assertClientIsInstanceOf(CloseableProducer.class, () -> tester.producer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), Map.of()));
        }
    }

    @Test
    void shouldReturnDifferentInstancesOfConsumer(KafkaCluster cluster) {
        // Given
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            assertDistinctInstanceOf(tester::consumer);
            assertDistinctInstanceOf(() -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER));
            assertDistinctInstanceOf(() -> tester.consumer(Map.of()));
            assertDistinctInstanceOf(() -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
            assertDistinctInstanceOf(() -> tester.consumer(Serdes.String(), Serdes.String(), Map.of()));
            assertDistinctInstanceOf(() -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), Map.of()));
        }
    }

    @Test
    void shouldReturnClosableConsumer(KafkaCluster cluster) {
        // Given
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            // Then
            assertClientIsInstanceOf(CloseableConsumer.class, tester::consumer);
            assertClientIsInstanceOf(CloseableConsumer.class, () -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER));
            assertClientIsInstanceOf(CloseableConsumer.class, () -> tester.consumer(Map.of()));
            assertClientIsInstanceOf(CloseableConsumer.class, () -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
            assertClientIsInstanceOf(CloseableConsumer.class, () -> tester.consumer(Serdes.String(), Serdes.String(), Map.of()));
            assertClientIsInstanceOf(CloseableConsumer.class, () -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), Map.of()));
        }
    }

    @Test
    void testConsumerMethods(KafkaCluster cluster) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(cluster.getKafkaClientConfiguration(), new StringSerializer(), new StringSerializer());
        producer.send(new ProducerRecord<>(TOPIC, "key", "value")).get(10, TimeUnit.SECONDS);
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            withConsumer(tester::consumer, KroxyliciousTestersTest::assertOneRecordConsumedFrom);
            withConsumer(() -> tester.consumer(randomGroupIdAndEarliestReset()), KroxyliciousTestersTest::assertOneRecordConsumedFrom);
            withConsumer(() -> tester.consumer(Serdes.String(), Serdes.String(), randomGroupIdAndEarliestReset()), KroxyliciousTestersTest::assertOneRecordConsumedFrom);
            withConsumer(() -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER), KroxyliciousTestersTest::assertOneRecordConsumedFrom);
            withConsumer(() -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER, randomGroupIdAndEarliestReset()), KroxyliciousTestersTest::assertOneRecordConsumedFrom);
            withConsumer(() -> tester.consumer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), randomGroupIdAndEarliestReset()),
                    KroxyliciousTestersTest::assertOneRecordConsumedFrom);
        }
    }

    private void withConsumer(Supplier<Consumer<String, String>> supplier, java.util.function.Consumer<Consumer<String, String>> consumerFunc) {
        try (Consumer<String, String> consumer = supplier.get()) {
            consumerFunc.accept(consumer);
        }
    }

    @Test
    void testProducerMethods(KafkaCluster cluster) throws Exception {
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            send(tester.producer());
            send(tester.producer(Map.of()));
            send(tester.producer(Serdes.String(), Serdes.String(), Map.of()));
            send(tester.producer(DEFAULT_VIRTUAL_CLUSTER));
            send(tester.producer(DEFAULT_VIRTUAL_CLUSTER, Map.of()));
            send(tester.producer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), Map.of()));

            HashMap<String, Object> config = new HashMap<>(cluster.getKafkaClientConfiguration());
            config.putAll(randomGroupIdAndEarliestReset());
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
            consumer.subscribe(List.of(TOPIC));
            int recordCount = consumer.poll(Duration.ofSeconds(10)).count();
            assertEquals(6, recordCount);
        }
    }

    @Test
    void testRestartingProxyDoesNotCloseClients(KafkaCluster cluster) throws Exception {
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            var admin = tester.admin();
            var producer = tester.producer();
            var consumer = tester.consumer();
            assertThat(admin.describeCluster()).isNotNull();
            send(producer);
            consumer.subscribe(List.of(TOPIC));
            assertThat(consumer.poll(Duration.ofSeconds(10))).isNotNull();
            tester.restartProxy();
            // assert some basic things here but if restarting the proxy restarted the clients these would except
            assertThat(admin.describeCluster()).isNotNull();
            send(producer);
            assertThat(consumer.poll(Duration.ofSeconds(10))).isNotNull();
        }
    }

    @Test
    void testClosingTesterAlsoClosesClients(KafkaCluster cluster) throws Exception {
        var tester = kroxyliciousTester(proxy(cluster));
        var admin = tester.admin();
        var producer = tester.producer();
        var consumer = tester.consumer();
        assertThat(admin.describeCluster()).isNotNull();
        send(producer);
        consumer.subscribe(List.of(TOPIC));
        assertThat(consumer.poll(Duration.ofSeconds(10))).isNotNull();
        tester.close();
        // we expect the following to throw an exception, but if it doesn't we fail.
        try {
            admin.describeCluster();
            send(producer);
            consumer.poll(Duration.ofSeconds(10));
            fail("No exception was thrown when invoking clients that should be closed");
        }
        catch (Exception e) {
            assertThat(e).isNotNull();
        }
    }

    @Test
    void testCanCloseTesterWithClosedClient(KafkaCluster cluster) {
        try {
            var tester = kroxyliciousTester(proxy(cluster));
            var admin = tester.admin();
            admin.close();
            tester.close();
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testMockRequestMockTester() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy)) {
            assertCanSendRequestsAndReceiveMockResponses(tester, tester::simpleTestClient);
            tester.clearMock();
            assertCanSendRequestsAndReceiveMockResponses(tester, () -> tester.simpleTestClient(DEFAULT_VIRTUAL_CLUSTER));
        }
    }

    @Test
    void testMockRequestInitialRequestCount() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy)) {
            assertThat(tester.getReceivedRequestCount()).isZero();
        }
    }

    @Test
    void testSimpleTestClientReportsConnectionState() {
        try (var tester = mockKafkaKroxyliciousTester(KroxyliciousConfigUtils::proxy);
                var kafkaClient = tester.simpleTestClient()) {
            assertThat(kafkaClient.isOpen()).isFalse();

            var mockResponse = new DescribeAclsResponseData().setErrorMessage("hello").setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.DESCRIBE_ACLS, ApiKeys.DESCRIBE_ACLS.latestVersion(), mockResponse));

            var response = kafkaClient
                    .getSync(new Request(ApiKeys.DESCRIBE_ACLS, ApiKeys.DESCRIBE_ACLS.latestVersion(), "client", new DescribeAclsRequestData()));
            assertThat(response.payload().message()).isEqualTo(mockResponse);
            assertThat(kafkaClient.isOpen()).isTrue();
        }
    }

    @Test
    void testIllegalToAskForNonExistentVirtualCluster(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(proxy(cluster))) {
            assertThrows(IllegalArgumentException.class, () -> tester.simpleTestClient("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.consumer("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.consumer("NON_EXIST", Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.consumer("NON_EXIST", Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.producer("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.producer("NON_EXIST", Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.producer("NON_EXIST", Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.admin("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.admin("NON_EXIST", Map.of()));
        }
    }

    @Test
    void testIllegalToAskForDefaultClientsWhenVirtualClustersAmbiguous(KafkaCluster cluster) {
        String clusterBootstrapServers = cluster.getBootstrapServers();
        ConfigurationBuilder builder = new ConfigurationBuilder();
        ConfigurationBuilder proxy = addVirtualCluster(clusterBootstrapServers, addVirtualCluster(clusterBootstrapServers, builder, "foo",
                "localhost:9192"), "bar", "localhost:9296");
        try (var tester = kroxyliciousTester(proxy)) {
            assertThrows(AmbiguousVirtualClusterException.class, tester::simpleTestClient);
            assertThrows(AmbiguousVirtualClusterException.class, tester::consumer);
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.consumer(Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.consumer(Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, tester::producer);
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.producer(Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.producer(Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, tester::admin);
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.admin(Map.of()));
        }
    }

    private static ConfigurationBuilder addVirtualCluster(String clusterBootstrapServers, ConfigurationBuilder builder, String clusterName,
                                                          String defaultProxyBootstrap) {
        return builder.addToVirtualClusters(clusterName, new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(clusterBootstrapServers)
                .endTargetCluster()
                .addToListeners(defaultPortIdentifiesNodeListenerBuilder(HostPort.parse(defaultProxyBootstrap)).build())
                .build());
    }

    private static void assertCanSendRequestsAndReceiveMockResponses(MockServerKroxyliciousTester tester, Supplier<KafkaClient> kafkaClientSupplier) {
        try (var kafkaClient = kafkaClientSupplier.get()) {
            var mockResponse1 = new DescribeAclsResponseData().setErrorMessage("hello").setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.DESCRIBE_ACLS, ApiKeys.DESCRIBE_ACLS.latestVersion(), mockResponse1));

            var mockResponse2 = new ListTransactionsResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.LIST_TRANSACTIONS, ApiKeys.LIST_TRANSACTIONS.latestVersion(), mockResponse2));

            Request describeAcls = new Request(ApiKeys.DESCRIBE_ACLS, ApiKeys.DESCRIBE_ACLS.latestVersion(), "client", new DescribeAclsRequestData());
            var response1 = kafkaClient.getSync(describeAcls);
            ApiMessage response1Message = response1.payload().message();
            assertThat(response1Message).isInstanceOf(DescribeAclsResponseData.class);
            assertThat(response1Message).isEqualTo(mockResponse1);
            assertThat(tester.getOnlyRequestForApiKey(ApiKeys.DESCRIBE_ACLS)).isEqualTo(describeAcls);
            assertThat(tester.getRequestsForApiKey(ApiKeys.DESCRIBE_ACLS)).containsOnly(describeAcls);
            assertThat(tester.getReceivedRequestCount()).isEqualTo(1);

            Request listTransactions = new Request(ApiKeys.LIST_TRANSACTIONS, ApiKeys.LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData());
            var response2 = kafkaClient.getSync(listTransactions);
            ApiMessage response2Message = response2.payload().message();
            assertInstanceOf(ListTransactionsResponseData.class, response2Message);
            assertThat(response2Message).isInstanceOf(ListTransactionsResponseData.class);
            assertThat(response2Message).isEqualTo(mockResponse2);
            assertThat(tester.getOnlyRequestForApiKey(ApiKeys.LIST_TRANSACTIONS)).isEqualTo(listTransactions);
            assertThat(tester.getRequestsForApiKey(ApiKeys.LIST_TRANSACTIONS)).containsOnly(listTransactions);
            assertThat(tester.getReceivedRequestCount()).isEqualTo(2);
        }
    }

    private static void send(Producer<String, String> producer) throws Exception {
        producer.send(new ProducerRecord<>(TOPIC, "key", "value")).get(10, TimeUnit.SECONDS);
    }

    @NonNull
    private static Map<String, Object> randomGroupIdAndEarliestReset() {
        return Map.of(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static void assertOneRecordConsumedFrom(Consumer<String, String> consumer) {
        consumer.subscribe(List.of(TOPIC));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());
    }

    private <T> void assertClientIsInstanceOf(Class<? extends T> expectedClass, Supplier<? extends T> clientSupplier) {
        final T client = clientSupplier.get();
        assertThat(client).isInstanceOf(expectedClass);
    }

    private <T> void assertDistinctInstanceOf(Supplier<? extends T> clientSupplier) {
        final T client = clientSupplier.get();
        final T otherClient = clientSupplier.get();
        assertThat(otherClient).isNotNull();
        assertThat(client).isNotNull().isNotSameAs(otherClient);
    }
}