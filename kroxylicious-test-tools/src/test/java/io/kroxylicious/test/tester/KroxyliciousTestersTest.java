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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.KroxyliciousConfig;
import io.kroxylicious.proxy.KroxyliciousConfigBuilder;
import io.kroxylicious.proxy.VirtualClusterBuilder;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.withDefaultFilters;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(KafkaClusterExtension.class)
public class KroxyliciousTestersTest {

    public static final String TOPIC = "example";

    @Test
    public void testAdminMethods(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(withDefaultFilters(proxy(cluster)))) {
            assertNotNull(tester.admin().describeCluster().clusterId().get(10, TimeUnit.SECONDS));
            assertNotNull(tester.admin(Map.of()).describeCluster().clusterId().get(10, TimeUnit.SECONDS));
            assertNotNull(tester.admin(DEFAULT_VIRTUAL_CLUSTER, Map.of()).describeCluster().clusterId().get(10, TimeUnit.SECONDS));
        }
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testConsumerMethods(KafkaCluster cluster) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(cluster.getKafkaClientConfiguration(), new StringSerializer(), new StringSerializer());
        producer.send(new ProducerRecord<>(TOPIC, "key", "value")).get(10, TimeUnit.SECONDS);
        try (var tester = kroxyliciousTester(withDefaultFilters(proxy(cluster)))) {
            assertOneRecordConsumedFrom(tester.consumer());
            assertOneRecordConsumedFrom(tester.consumer(randomGroupIdAndEarliestReset()));
            assertOneRecordConsumedFrom(tester.consumer(Serdes.String(), Serdes.String(), randomGroupIdAndEarliestReset()));

            assertOneRecordConsumedFrom(tester.consumer(DEFAULT_VIRTUAL_CLUSTER));
            assertOneRecordConsumedFrom(tester.consumer(DEFAULT_VIRTUAL_CLUSTER, randomGroupIdAndEarliestReset()));
            assertOneRecordConsumedFrom(tester.consumer(DEFAULT_VIRTUAL_CLUSTER, Serdes.String(), Serdes.String(), randomGroupIdAndEarliestReset()));
        }
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testProducerMethods(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(withDefaultFilters(proxy(cluster)))) {
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
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testSingleRequestClient(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(withDefaultFilters(proxy(cluster)))) {
            assertCanSendSingleRequestAndGetResponse(tester.singleRequestClient());
            assertCanSendSingleRequestAndGetResponse(tester.singleRequestClient(DEFAULT_VIRTUAL_CLUSTER));
        }
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testMockTester() {
        try (var tester = mockKafkaKroxyliciousTester(clusterBootstrapServers -> withDefaultFilters(proxy(clusterBootstrapServers)))) {
            assertCanSendSingleRequestAndReceiveMockMessage(tester, tester.singleRequestClient());
            assertCanSendSingleRequestAndReceiveMockMessage(tester, tester.singleRequestClient(DEFAULT_VIRTUAL_CLUSTER));
        }
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testIllegalToAskForNonExistantVirtualCluster(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(withDefaultFilters(proxy(cluster)))) {
            assertThrows(IllegalArgumentException.class, () -> tester.singleRequestClient("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.consumer("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.consumer("NON_EXIST", Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.consumer("NON_EXIST", Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.producer("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.producer("NON_EXIST", Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.producer("NON_EXIST", Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(IllegalArgumentException.class, () -> tester.admin("NON_EXIST"));
            assertThrows(IllegalArgumentException.class, () -> tester.admin("NON_EXIST", Map.of()));
        }
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testIllegalToAskForDefaultClientsWhenVirtualClustersAmbiguous(KafkaCluster cluster) {
        String clusterBootstrapServers = cluster.getBootstrapServers();
        KroxyliciousConfigBuilder builder = KroxyliciousConfig.builder();
        KroxyliciousConfigBuilder proxy = addVirtualCluster(clusterBootstrapServers, addVirtualCluster(clusterBootstrapServers, builder, "foo",
                "localhost:9192"), "bar", "localhost:9193");
        try (var tester = kroxyliciousTester(withDefaultFilters(proxy))) {
            assertThrows(AmbiguousVirtualClusterException.class, tester::singleRequestClient);
            assertThrows(AmbiguousVirtualClusterException.class, tester::consumer);
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.consumer(Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.consumer(Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, tester::producer);
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.producer(Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.producer(Serdes.String(), Serdes.String(), Map.of()));
            assertThrows(AmbiguousVirtualClusterException.class, tester::admin);
            assertThrows(AmbiguousVirtualClusterException.class, () -> tester.admin(Map.of()));
        }
        catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    private static KroxyliciousConfigBuilder addVirtualCluster(String clusterBootstrapServers, KroxyliciousConfigBuilder builder, String clusterName,
                                                               String defaultProxyBootstrap) {
        return builder.addToVirtualClusters(clusterName, new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(clusterBootstrapServers)
                .endTargetCluster()
                .withNewClusterEndpointConfigProvider()
                .withType("StaticCluster")
                .withConfig(Map.of("bootstrapAddress", defaultProxyBootstrap))
                .endClusterEndpointConfigProvider()
                .build());
    }

    private static void assertCanSendSingleRequestAndReceiveMockMessage(MockServerKroxyliciousTester tester, KafkaClient kafkaClient) {
        DescribeAclsResponseData data = new DescribeAclsResponseData();
        data.setErrorMessage("helllo");
        data.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        tester.setMockResponse(new Response(ApiKeys.DESCRIBE_ACLS, ApiKeys.DESCRIBE_ACLS.latestVersion(), data));
        Response response = kafkaClient
                .getSync(new Request(ApiKeys.DESCRIBE_ACLS, ApiKeys.DESCRIBE_ACLS.latestVersion(), "client", new DescribeAclsRequestData()));
        assertInstanceOf(DescribeAclsResponseData.class, response.message());
        assertEquals(data, response.message());
    }

    private static void assertCanSendSingleRequestAndGetResponse(KafkaClient kafkaClient) {
        Response response = kafkaClient
                .getSync(new Request(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(), "client", new ApiVersionsRequestData()));
        assertInstanceOf(ApiVersionsResponseData.class, response.message());
    }

    private static void send(Producer<String, String> producer) throws InterruptedException, ExecutionException, TimeoutException {
        producer.send(new ProducerRecord<>(TOPIC, "key", "value")).get(10, TimeUnit.SECONDS);
    }

    @NotNull
    private static Map<String, Object> randomGroupIdAndEarliestReset() {
        return Map.of(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static void assertOneRecordConsumedFrom(Consumer<String, String> consumer) {
        consumer.subscribe(List.of(TOPIC));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());
    }

}