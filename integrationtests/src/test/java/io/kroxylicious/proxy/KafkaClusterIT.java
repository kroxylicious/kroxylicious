/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.testkafkacluster.ContainerBasedKafkaCluster;
import io.kroxylicious.proxy.testkafkacluster.KafkaCluster;
import io.kroxylicious.proxy.testkafkacluster.KafkaClusterConfig;
import io.kroxylicious.proxy.testkafkacluster.KafkaClusterFactory;
import io.kroxylicious.proxy.testkafkacluster.KeytoolCertificateGenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test case that simply exercises the ability to control the kafka cluster from the test.
 * It intentional that this test does not involve the proxy.
 */
public class KafkaClusterIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterIT.class);
    private TestInfo testInfo;
    private KeytoolCertificateGenerator keytoolCertificateGenerator;

    @Test
    public void kafkaClusterKraftMode() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(true)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaClusterZookeeperMode() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(false)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaTwoNodeClusterKraftMode() throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(true)
                .build())) {
            assumeTrue(cluster instanceof ContainerBasedKafkaCluster, "KAFKA-14287: kraft timing out on shutdown in multinode case");
            cluster.start();
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @Test
    public void kafkaTwoNodeClusterZookeeperMode() throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(false)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @Test
    public void kafkaClusterKraftModeWithAuth() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .kraftMode(true)
                .testInfo(testInfo)
                .securityProtocol("SASL_PLAINTEXT")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaClusterZookeeperModeWithAuth() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(false)
                .securityProtocol("SASL_PLAINTEXT")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaClusterKraftModeSASL_SSL() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .keytoolCertificateGenerator(keytoolCertificateGenerator)
                .kraftMode(true)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaClusterKraftModeSSL() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .keytoolCertificateGenerator(keytoolCertificateGenerator)
                .kraftMode(true)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaClusterZookeeperModeSASL_SSL() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .keytoolCertificateGenerator(keytoolCertificateGenerator)
                .kraftMode(false)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    public void kafkaClusterZookeeperModeSSL() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .keytoolCertificateGenerator(keytoolCertificateGenerator)
                .kraftMode(false)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    private void verifyRecordRoundTrip(int expected, KafkaCluster cluster) throws Exception {
        var topic = "TOPIC_1";
        var message = "Hello, world!";

        try (var admin = KafkaAdminClient.create(cluster.getKafkaClientConfiguration())) {
            assertEquals(expected, getActualNumberOfBrokers(admin));
            var rf = (short) Math.min(1, Math.max(expected, 3));
            createTopic(admin, topic, rf);
        }

        produce(cluster, topic, message);
        consume(cluster, topic, "Hello, world!");
    }

    private void produce(KafkaCluster cluster, String topic, String message) throws Exception {
        Map<String, Object> config = cluster.getKafkaClientConfiguration();
        config.putAll(Map.<String, Object> of(
                ProducerConfig.CLIENT_ID_CONFIG, "myclient",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        try (var producer = new KafkaProducer<String, String>(config)) {
            producer.send(new ProducerRecord<>(topic, "my-key", message)).get();
        }
    }

    private void consume(KafkaCluster cluster, String topic, String message) throws Exception {
        Map<String, Object> config = cluster.getKafkaClientConfiguration();
        config.putAll(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(Set.of(topic));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            assertEquals(message, records.iterator().next().value());
            consumer.unsubscribe();
        }
    }

    private int getActualNumberOfBrokers(AdminClient admin) throws Exception {
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        return describeClusterResult.nodes().get().size();
    }

    private void createTopic(AdminClient admin1, String topic, short replicationFactor) throws Exception {
        admin1.createTopics(List.of(new NewTopic(topic, 1, replicationFactor))).all().get();
    }

    @BeforeEach
    void before(TestInfo testInfo) throws IOException {
        this.testInfo = testInfo;
        this.keytoolCertificateGenerator = new KeytoolCertificateGenerator();
    }

    @AfterEach
    void after() {
        Path filePath = Paths.get(keytoolCertificateGenerator.getCertLocation());
        filePath.toFile().deleteOnExit();
    }
}
