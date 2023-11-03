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
import java.util.UUID;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(KafkaClusterExtension.class)
class SampleFilterIntegrationTest {

    // Configure filters here
    private static final String FIND_CONFIG_FIELD = "findValue";
    private static final String REPLACE_CONFIG_FIELD = "replacementValue";
    private static final TestFilter SAMPLE_PRODUCE_REQUEST_FILTER = new TestFilter(SampleProduceRequestFilterFactory.class.getName(),
            Map.of(FIND_CONFIG_FIELD, "foo", REPLACE_CONFIG_FIELD, "bar"));
    private static final TestFilter SAMPLE_FETCH_RESPONSE_FILTER = new TestFilter(SampleFetchResponseFilterFactory.class.getName(),
            Map.of(FIND_CONFIG_FIELD, "bar", REPLACE_CONFIG_FIELD, "baz"));

    // Configure test input/expected values here
    private static final String NO_TRANSFORM_VALUE = "sample";
    private static final String PRE_TRANSFORM_VALUE = "foo bar baz";
    private static final String FETCH_TRANSFORM_VALUE = "foo baz baz";
    private static final String PRODUCE_TRANSFORM_VALUE = "bar bar baz";

    // Configure Cluster/Producer/Consumer values here
    private static final Integer TIMEOUT_SECONDS = 10;
    private static final Integer TOPIC_PARTITIONS = 1;
    private static final Short TOPIC_REPLICATION = 1;

    @BrokerCluster
    KafkaCluster cluster;

    FilterIntegrationTest test;

    @AfterEach
    public void afterEach() {
        test.close();
    }

    /**
     * Test that the SampleProduceRequestFilter will transform when given data containing its findValue.
     */
    @Test
    void sampleProduceRequestFilterWillTransformIntegrationTest() {
        test = new FilterIntegrationTest(SAMPLE_PRODUCE_REQUEST_FILTER);
        test.withTopic("sampleProduceRequestFilterWillTransformRoundTripTest")
                .produceMessage(PRE_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .assertConsumerRecordEquals(PRODUCE_TRANSFORM_VALUE);
    }

    /**
     * Test that the SampleProduceRequestFilter won't transform when given data that does not contain its findValue.
     */
    @Test
    void sampleProduceRequestFilterWontTransformIntegrationTest() {
        test = new FilterIntegrationTest(SAMPLE_PRODUCE_REQUEST_FILTER);
        test.withTopic("sampleProduceRequestFilterWontTransformRoundTripTest")
                .produceMessage(NO_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .assertConsumerRecordEquals(NO_TRANSFORM_VALUE);
    }

    /**
     * Test that the SampleProduceRequestFilter won't drop a second message produced to a topic.
     */
    @Test
    void sampleProduceRequestFilterWontDropSecondMessageIntegrationTest() {
        test = new FilterIntegrationTest(SAMPLE_PRODUCE_REQUEST_FILTER);
        test.withTopic("sampleProduceRequestFilterWontDropSecondMessageRoundTripTest")
                .produceMessage(NO_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .produceMessage(PRE_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .assertConsumerRecordEquals(PRODUCE_TRANSFORM_VALUE);
    }

    /**
     * Test that the SampleFetchResponseFilter will transform when given data containing its findValue.
     */
    @Test
    void sampleFetchResponseFilterWillTransformIntegrationTest() {
        test = new FilterIntegrationTest(SAMPLE_FETCH_RESPONSE_FILTER);
        test.withTopic("sampleFetchResponseFilterWillTransformRoundTripTest")
                .produceMessage(PRE_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .assertConsumerRecordEquals(FETCH_TRANSFORM_VALUE);
    }

    /**
     * Test that the SampleFetchResponseFilter won't transform when given data that does not contain its findValue.
     */
    @Test
    void sampleFetchResponseFilterWontTransformIntegrationTest() {
        test = new FilterIntegrationTest(SAMPLE_FETCH_RESPONSE_FILTER);
        test.withTopic("sampleFetchResponseFilterWontTransformRoundTripTest")
                .produceMessage(NO_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .assertConsumerRecordEquals(NO_TRANSFORM_VALUE);
    }

    /**
     * Test that the SampleFetchResponseFilter won't drop a second message produced to a topic.
     */
    @Test
    void sampleFetchResponseFilterWontDropSecondMessageIntegrationTest() {
        test = new FilterIntegrationTest(SAMPLE_FETCH_RESPONSE_FILTER);
        test.withTopic("sampleFetchResponseFilterWontDropSecondMessageRoundTripTest")
                .produceMessage(NO_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .produceMessage(PRE_TRANSFORM_VALUE)
                .consumeSingleRecord()
                .assertConsumerRecordEquals(FETCH_TRANSFORM_VALUE);
    }

    /**
     * Reusable class for running filter integration tests.
     */
    private class FilterIntegrationTest {
        private final KroxyliciousTester tester;
        private final Producer<String, String> producer;
        private final Consumer<String, byte[]> consumer;
        private final Admin admin;
        private String topic;
        private ConsumerRecord<String, byte[]> record;

        /**
         * Creates a test object.
         * @param filters the filters to be used in the test
         */
        FilterIntegrationTest(TestFilter... filters) {
            ConfigurationBuilder builder = proxy(cluster);
            for (TestFilter filter : filters) {
                builder.addToFilters(new FilterDefinitionBuilder(filter.name()).withConfig(filter.config()).build());
            }
            tester = kroxyliciousTester(builder);
            producer = tester.producer();
            consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(),
                    Map.of(ConsumerConfig.GROUP_ID_CONFIG, "group-id-0", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
            admin = tester.admin();
        }

        /**
         * Create a topic with the given name for this test.
         * @param name the name of the topic to create
         * @return the SingleFilterIntegrationTest object (itself)
         */
        FilterIntegrationTest withTopic(String name) {
            try {
                admin.createTopics(List.of(new NewTopic(name, TOPIC_PARTITIONS, TOPIC_REPLICATION))).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.topic = name;
            return this;
        }

        /**
         * Produces the given value as a message to the test Kroxylicious instance.
         * @param value the value to be produced
         * @return the SingleFilterIntegrationTest object (itself)
         */
        FilterIntegrationTest produceMessage(String value) {
            this.ensureTopicExists();
            try {
                this.producer.send(new ProducerRecord<>(this.topic, value)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        /**
         * Consume a single record from the test's topic.
         * @return the SingleFilterIntegrationTest object (itself)
         */
        FilterIntegrationTest consumeSingleRecord() {
            if (this.topic == null) {
                fail("Could not consumeSingleRecord - this test has no topic");
            }
            this.consumer.subscribe(List.of(this.topic));
            ConsumerRecords<String, byte[]> poll = this.consumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));
            if (poll.count() == 0) {
                fail(String.format("No records could be consumed from topic: %s.", this.topic));
            }
            this.record = poll.records(this.topic).iterator().next();
            return this;
        }

        /**
         * Assert that the string value of the Consumer Record last consumed from this test's topic equals the given value.
         * @param value the value to match
         */
        void assertConsumerRecordEquals(String value) {
            if (this.record == null) {
                fail("Could not assertConsumerRecordEquals - this test has no record");
            }
            String recordValue = new String(this.record.value(), StandardCharsets.UTF_8);
            assertEquals(value, recordValue);
        }

        /**
         * Closes this test's KroxyliciousTester. Should be called after each test is concluded.
         */
        void close() {
            this.tester.close();
            this.admin.close();
            this.producer.close();
            this.consumer.close();
        }

        /**
         * Creates a topic with a random UUID name if none exists for this test.
         */
        private void ensureTopicExists() {
            if (this.topic == null) {
                this.withTopic(UUID.randomUUID().toString());
            }
        }
    }

    private record TestFilter(String name, Map<String, Object> config) {}
}
