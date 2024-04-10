/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.schema.validation;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import io.apicurio.registry.resolver.DefaultSchemaResolver;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;

import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.schema.ProduceValidationFilterFactory;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
class ApicurioSchemaValidationIT extends BaseIT {

    private static final String SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";
    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);
    private static final GenericRecord RECORD = createAvroRecord();
    private static final String TOPIC_NAME = "my-test-topic";
    private static final String SUBJECT_NAME = "Greeting";
    public static final String APICURIO_VERSION = "2.5.10.Final";
    private final GenericContainer<?> container = new GenericContainer<>("apicurio/apicurio-registry-mem:" + APICURIO_VERSION)
            .withExposedPorts(8080)
            .waitingFor(new HostPortWaitStrategy().forPorts(8080));

    @BeforeEach
    public void start() {
        container.start();
    }

    @AfterEach
    public void stop() {
        container.stop();
    }

    @Test
    void testValueWithNoSchemaHeaderRejected(KafkaCluster cluster, Admin admin) {
        createTopic(admin, TOPIC_NAME, 1);
        var config = proxyWithApicurioValidationForTopic(cluster, true);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            ProducerRecord<String, String> recordWithNoSchema = new ProducerRecord<>(TOPIC_NAME, "arbitrary", "arbitrary");
            Future<RecordMetadata> sendFuture = producer.send(recordWithNoSchema);
            assertFutureCompletedExceptionally(sendFuture, "Value was invalid: record headers did not contain: apicurio.value.globalId");
        }
    }

    @Test
    void testKeyWithNoSchemaHeaderRejected(KafkaCluster cluster, Admin admin) {
        createTopic(admin, TOPIC_NAME, 1);
        var config = proxyWithApicurioValidationForTopic(cluster, false);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            ProducerRecord<String, String> recordWithNoSchema = new ProducerRecord<>(TOPIC_NAME, "arbitrary", "arbitrary");
            Future<RecordMetadata> sendFuture = producer.send(recordWithNoSchema);
            assertFutureCompletedExceptionally(sendFuture, "Key was invalid: record headers did not contain: apicurio.key.globalId");
        }
    }

    @Test
    void testNullRecordAllowed(KafkaCluster cluster, Admin admin) {
        createTopic(admin, TOPIC_NAME, 1);
        var config = proxyWithApicurioValidationForTopic(cluster, true);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            ProducerRecord<String, String> recordWithNullValue = new ProducerRecord<>(TOPIC_NAME, "arbitrary", null);
            Future<RecordMetadata> sendFuture = producer.send(recordWithNullValue);
            assertThat(sendFuture).succeedsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testNullKeyAllowed(KafkaCluster cluster, Admin admin) {
        createTopic(admin, TOPIC_NAME, 1);
        var config = proxyWithApicurioValidationForTopic(cluster, false);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            ProducerRecord<String, String> recordWithNullKey = new ProducerRecord<>(TOPIC_NAME, null, null);
            Future<RecordMetadata> sendFuture = producer.send(recordWithNullKey);
            assertThat(sendFuture).succeedsWithin(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testValueEncodedWithSchemaHeaderAccepted(KafkaCluster cluster, Admin admin) {
        createTopic(admin, TOPIC_NAME, 1);
        var config = proxyWithApicurioValidationForTopic(cluster, true);
        try (var tester = kroxyliciousTester(config)) {
            try (var producer = apicurioAvroValueProducer(tester)) {
                ProducerRecord<Object, Object> recordWithAvroValue = new ProducerRecord<>(TOPIC_NAME, SUBJECT_NAME, RECORD);
                Future<RecordMetadata> sendFuture = producer.send(recordWithAvroValue);
                assertThat(sendFuture).succeedsWithin(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void testKeyEncodedWithSchemaHeaderAccepted(KafkaCluster cluster, Admin admin) {
        createTopic(admin, TOPIC_NAME, 1);

        var config = proxyWithApicurioValidationForTopic(cluster, false);
        try (var tester = kroxyliciousTester(config)) {
            try (var producer = apicurioAvroKeyProducer(tester)) {
                ProducerRecord<Object, Object> recordWithAvroKey = new ProducerRecord<>(TOPIC_NAME, RECORD, SUBJECT_NAME);
                Future<RecordMetadata> sendFuture = producer.send(recordWithAvroKey);
                assertThat(sendFuture).succeedsWithin(5, TimeUnit.SECONDS);
            }
        }
    }

    @NonNull
    private KafkaProducer<Object, Object> apicurioAvroValueProducer(KroxyliciousTester tester) {
        return apicurioProducer(tester, StringSerializer.class, AvroKafkaSerializer.class);
    }

    @NonNull
    private KafkaProducer<Object, Object> apicurioAvroKeyProducer(KroxyliciousTester tester) {
        return apicurioProducer(tester, AvroKafkaSerializer.class, StringSerializer.class);
    }

    @NonNull
    private <T extends Serializer<?>, S extends Serializer<?>> KafkaProducer<Object, Object> apicurioProducer(KroxyliciousTester tester,
                                                                                                              Class<T> keySerializer,
                                                                                                              Class<S> valueSerializer) {
        String registryUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8080) + "/apis/registry/v2";
        return new KafkaProducer<>(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, tester.getBootstrapAddress(),
                        SerdeConfig.REGISTRY_URL, registryUrl,
                        SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                        SerdeConfig.SCHEMA_RESOLVER, DefaultSchemaResolver.class,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer));
    }

    private static ConfigurationBuilder proxyWithApicurioValidationForTopic(KafkaCluster cluster, boolean targetValue) {
        return proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceValidationFilterFactory.class.getName())
                        .withConfig("rules",
                                List.of(Map.of("topicNames", List.of(TOPIC_NAME), targetValue ? "valueRule" : "keyRule",
                                        Map.of("allowsNulls", true, "hasApicurioSchema", Map.of()))))
                        .build());
    }

    @NonNull
    private static GenericRecord createAvroRecord() {
        GenericRecord record = new GenericData.Record(SCHEMA);
        Date now = new Date();
        String message = "Hello!";
        record.put("Message", message);
        record.put("Time", now.getTime());
        return record;
    }

    private static void assertFutureCompletedExceptionally(Future<RecordMetadata> invalid, String message) {
        assertThat(invalid).failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(InvalidRecordException.class)
                .havingCause().withMessageContaining(message);
    }

}
