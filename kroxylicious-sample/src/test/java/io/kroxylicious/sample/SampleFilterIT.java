/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.withDefaultFilters;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(KafkaClusterExtension.class)
public class SampleFilterIT {

    private static final String PRE_TRANSFORM_VALUE = "foo";
    private static final String NO_TRANSFORM_VALUE = "sample";
    private static final String PRODUCE_TRANSFORM_VALUE = "bar";
    private static final String FETCH_TRANSFORM_VALUE = "baz";
    private static final String FIND_CONFIG_FIELD = "findValue";
    private static final String REPLACE_CONFIG_FIELD = "replaceValue";
    private static final Map<String, Object> PRODUCE_CONFIG = Map.of(FIND_CONFIG_FIELD, "foo", REPLACE_CONFIG_FIELD, "bar");
    private static final Map<String, Object> FETCH_CONFIG = Map.of(FIND_CONFIG_FIELD, "bar", REPLACE_CONFIG_FIELD, "baz");
    private static final Integer TIMEOUT_SECONDS = 10;
    private static final String TOPIC_NAME = "test";
    private static final Integer TOPIC_PARTITIONS = 1;
    private static final Short TOPIC_REPLICATION = 1;

    KafkaCluster cluster;
    private KroxyliciousTester tester;

    @BeforeEach
    public void beforeEach() {
        tester = kroxyliciousTester(withDefaultFilters(proxy(cluster))
                .addNewFilter().withType(SampleContributor.SAMPLE_PRODUCE).addToConfig(PRODUCE_CONFIG).endFilter()
                .addNewFilter().withType(SampleContributor.SAMPLE_FETCH).addToConfig(FETCH_CONFIG).endFilter());
    }

    @AfterEach
    public void afterEach() {
        tester.close();
    }

    @Test
    public void sampleFilterWillTransformRoundTripTest(Admin admin) {
        try (Producer<String, String> producer = tester.producer();
                Consumer<String, byte[]> kafkaClusterConsumer = new KafkaConsumer<>(mergeMaps(cluster.getKafkaClientConfiguration(),
                        Map.of(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                ByteArrayDeserializer.class, ConsumerConfig.GROUP_ID_CONFIG, "group-id-0", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")));
                Consumer<String, byte[]> proxyConsumer = tester.consumer(Serdes.String(), Serdes.ByteArray(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "group-id-1", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            admin.createTopics(List.of(new NewTopic(TOPIC_NAME, TOPIC_PARTITIONS, TOPIC_REPLICATION))).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME, PRE_TRANSFORM_VALUE)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // get from cluster
            kafkaClusterConsumer.subscribe(List.of(TOPIC_NAME));
            ConsumerRecords<String, byte[]> kafkaClusterPoll = kafkaClusterConsumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));
            if (kafkaClusterPoll.count() != 1) {
                fail(String.format("Sent 1 record but received %d records from Kafka Cluster Consumer.", kafkaClusterPoll.count()));
            }
            ConsumerRecord<String, byte[]> clusterRecord = kafkaClusterPoll.records(TOPIC_NAME).iterator().next();
            // check cluster record value is correct
            assertEquals(PRODUCE_TRANSFORM_VALUE, new String(clusterRecord.value(), StandardCharsets.UTF_8));
            // get from proxy
            proxyConsumer.subscribe(List.of(TOPIC_NAME));
            ConsumerRecords<String, byte[]> proxyPoll = proxyConsumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));
            if (proxyPoll.count() != 1) {
                fail(String.format("Sent 1 record but received %d records from Proxy Consumer.", proxyPoll.count()));
            }
            ConsumerRecord<String, byte[]> proxyRecord = proxyPoll.records(TOPIC_NAME).iterator().next();
            // check proxy record is correct
            assertEquals(FETCH_TRANSFORM_VALUE, new String(proxyRecord.value(), StandardCharsets.UTF_8));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sampleFilterWontTransformRoundTripTest(Admin admin) {
        try (Producer<String, String> producer = tester.producer();
                Consumer<String, byte[]> kafkaClusterConsumer = new KafkaConsumer<>(mergeMaps(cluster.getKafkaClientConfiguration(),
                        Map.of(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                ByteArrayDeserializer.class, ConsumerConfig.GROUP_ID_CONFIG, "group-id-0", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")));
                Consumer<String, byte[]> proxyConsumer = tester.consumer(Serdes.String(), Serdes.ByteArray(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "group-id-1", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            admin.createTopics(List.of(new NewTopic(TOPIC_NAME, TOPIC_PARTITIONS, TOPIC_REPLICATION))).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME, NO_TRANSFORM_VALUE)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // get from cluster
            kafkaClusterConsumer.subscribe(List.of(TOPIC_NAME));
            ConsumerRecords<String, byte[]> kafkaClusterPoll = kafkaClusterConsumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));
            if (kafkaClusterPoll.count() != 1) {
                fail(String.format("Sent 1 record but received %d records from Kafka Cluster Consumer.", kafkaClusterPoll.count()));
            }
            ConsumerRecord<String, byte[]> clusterRecord = kafkaClusterPoll.records(TOPIC_NAME).iterator().next();
            // check cluster record value is correct
            assertEquals(NO_TRANSFORM_VALUE, new String(clusterRecord.value(), StandardCharsets.UTF_8));
            // get from proxy
            proxyConsumer.subscribe(List.of(TOPIC_NAME));
            ConsumerRecords<String, byte[]> proxyPoll = proxyConsumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));
            if (proxyPoll.count() != 1) {
                fail(String.format("Sent 1 record but received %d records from Proxy Consumer.", proxyPoll.count()));
            }
            ConsumerRecord<String, byte[]> proxyRecord = proxyPoll.records(TOPIC_NAME).iterator().next();
            // check proxy record is correct
            assertEquals(NO_TRANSFORM_VALUE, new String(proxyRecord.value(), StandardCharsets.UTF_8));
            // check cluster and proxy record values are the same
            assertArrayEquals(clusterRecord.value(), proxyRecord.value());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    private static Map<String, Object> mergeMaps(Map<String, Object>... maps) {
        Map<String, Object> combined = new HashMap<>();
        for (Map<String, Object> m : maps) {
            combined.putAll(m);
        }
        return combined;
    }
}
