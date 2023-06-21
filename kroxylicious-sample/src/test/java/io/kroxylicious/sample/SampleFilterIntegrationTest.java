/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.withDefaultFilters;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(KafkaClusterExtension.class)
public class SampleFilterIntegrationTest {

    private static final String PRE_TRANSFORM_VALUE = "foo";
    private static final String NO_TRANSFORM_VALUE = "sample";
    private static final String FETCH_TRANSFORM_VALUE = "baz";
    private static final String FIND_CONFIG_FIELD = "findValue";
    private static final String REPLACE_CONFIG_FIELD = "replacementValue";
    private static final Map<String, Object> PRODUCE_CONFIG = Map.of(FIND_CONFIG_FIELD, "foo", REPLACE_CONFIG_FIELD, "bar");
    private static final Map<String, Object> FETCH_CONFIG = Map.of(FIND_CONFIG_FIELD, "bar", REPLACE_CONFIG_FIELD, "baz");
    private static final Integer TIMEOUT_SECONDS = 10;
    private static final Integer TOPIC_PARTITIONS = 1;
    private static final Short TOPIC_REPLICATION = 1;

    @BrokerCluster
    KafkaCluster cluster;
    private KroxyliciousTester tester;
    private Producer<String, String> producer;
    private Consumer<String, byte[]> consumer;
    private Admin admin;

    @BeforeEach
    public void beforeEach() {
        tester = kroxyliciousTester(withDefaultFilters(proxy(cluster))
                .addToFilters(new FilterDefinitionBuilder(SampleContributor.SAMPLE_PRODUCE).withConfig(PRODUCE_CONFIG).build())
                .addToFilters(new FilterDefinitionBuilder(SampleContributor.SAMPLE_FETCH).withConfig(FETCH_CONFIG).build()));
        producer = tester.producer();
        consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), Map.of(ConsumerConfig.GROUP_ID_CONFIG, "group-id-0", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        admin = tester.admin();
    }

    @AfterEach
    public void afterEach() {
        tester.close();
    }

    @Test
    public void sampleFilterWillTransformRoundTripTest() {
        try {
            String topicName = "sampleFilterWillTransformRoundTripTest";
            withTopic(topicName);
            doProduce(topicName, PRE_TRANSFORM_VALUE);
            var record = consumeSingleRecordFrom(topicName);
            assertConsumerRecordHasValue(record, FETCH_TRANSFORM_VALUE);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sampleFilterWontTransformRoundTripTest() {
        String topicName = "sampleFilterWontTransformRoundTripTest";
        withTopic(topicName);
        doProduce(topicName, NO_TRANSFORM_VALUE);
        var record = consumeSingleRecordFrom(topicName);
        assertConsumerRecordHasValue(record, NO_TRANSFORM_VALUE);
    }

    private void withTopic(String name) {
        try {
            admin.createTopics(List.of(new NewTopic(name, TOPIC_PARTITIONS, TOPIC_REPLICATION))).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doProduce(String topic, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, value)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConsumerRecord<String, byte[]> consumeSingleRecordFrom(String topic) {
        consumer.subscribe(List.of(topic));
        ConsumerRecords<String, byte[]> poll = consumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));
        if (poll.count() != 1) {
            fail(String.format("Sent 1 record but received %d records from consumer.", poll.count()));
        }
        return poll.records(topic).iterator().next();
    }

    private void assertConsumerRecordHasValue(ConsumerRecord<String, byte[]> record, String value) {
        String recordValue = new String(record.value(), StandardCharsets.UTF_8);
        assertEquals(value, recordValue);
    }
}
