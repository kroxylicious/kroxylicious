/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.adapter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.kroxylicious.systemtests.api.client.ClientCapability;
import io.kroxylicious.systemtests.api.client.ClientDriver;
import io.kroxylicious.systemtests.api.client.ClientExecutionEnvironment;
import io.kroxylicious.systemtests.api.client.ConsumedRecord;
import io.kroxylicious.systemtests.api.client.KafkaClient;
import io.kroxylicious.systemtests.api.client.KafkaClientException;
import io.kroxylicious.systemtests.api.client.ProduceRecord;
import io.kroxylicious.systemtests.api.client.TopicPartition;

/**
 * Implements {@link KafkaClient} with the Apache Kafka Java client running inside the test JVM.
 * Supports every {@link ClientCapability}.
 * <p>
 * Each operation opens and closes its own client instance, so operations are independent of
 * one another, mirroring the behaviour of the on-cluster drivers.
 */
final class InProcessJavaKafkaClient implements KafkaClient {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Set<ClientCapability> CAPABILITIES = EnumSet.allOf(ClientCapability.class);

    @Override
    public ClientDriver driver() {
        return ClientDriver.JAVA;
    }

    @Override
    public ClientExecutionEnvironment executionEnvironment() {
        return ClientExecutionEnvironment.IN_PROCESS;
    }

    @Override
    public boolean supports(ClientCapability capability) {
        return CAPABILITIES.contains(capability);
    }

    @Override
    public void createTopic(String topic, String bootstrap, int partitions, int replicationFactor) {
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) replicationFactor))).all().get();
        }
        catch (ExecutionException | KafkaException e) {
            throw new KafkaClientException("Failed to create topic '" + topic + "'", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaClientException("Interrupted creating topic '" + topic + "'", e);
        }
    }

    @Override
    public void produce(String topic, String bootstrap, List<ProduceRecord> records, Map<String, String> config) {
        try (var producer = new KafkaProducer<>(producerConfig(bootstrap, config), new StringSerializer(), new StringSerializer())) {
            for (ProduceRecord record : records) {
                producer.send(toProducerRecord(topic, record)).get();
            }
        }
        catch (ExecutionException | KafkaException e) {
            throw new KafkaClientException("Failed to produce to topic '" + topic + "'", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaClientException("Interrupted producing to topic '" + topic + "'", e);
        }
    }

    @Override
    public void produceInTransaction(String topic, String bootstrap, List<ProduceRecord> records) {
        Map<String, Object> config = producerConfig(bootstrap, Map.of());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kroxylicious-systemtest-" + UUID.randomUUID());
        try (var producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            producer.initTransactions();
            producer.beginTransaction();
            List<Future<RecordMetadata>> sends = new ArrayList<>(records.size());
            for (ProduceRecord record : records) {
                sends.add(producer.send(toProducerRecord(topic, record)));
            }
            producer.commitTransaction();
            // commitTransaction() flushes, so every send has completed; surface any per-record failure
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
        }
        catch (ExecutionException | KafkaException e) {
            throw new KafkaClientException("Failed to produce transactionally to topic '" + topic + "'", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaClientException("Interrupted producing transactionally to topic '" + topic + "'", e);
        }
    }

    @Override
    public List<ConsumedRecord> consume(String topic, String bootstrap, int count, Duration timeout, String groupId, Map<String, String> config) {
        if (config.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalArgumentException("Specify the consumer group via groupId, not via the '" + ConsumerConfig.GROUP_ID_CONFIG + "' config property");
        }
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(1, count));
        consumerConfig.putAll(config);

        List<ConsumedRecord> consumed = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        try (var consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            while (consumed.size() < count && Instant.now().isBefore(deadline)) {
                Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
                for (var record : consumer.poll(POLL_INTERVAL)) {
                    if (consumed.size() == count) {
                        break;
                    }
                    consumed.add(new ConsumedRecord(record.topic(), record.partition(), record.offset(), record.key(), record.value(), headersOf(record)));
                    toCommit.put(new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }
                if (!toCommit.isEmpty()) {
                    consumer.commitSync(toCommit);
                }
            }
        }
        catch (KafkaException e) {
            throw new KafkaClientException("Failed to consume from topic '" + topic + "'", e);
        }
        return List.copyOf(consumed);
    }

    @Override
    public void commitOffsets(String groupId, String bootstrap, Map<TopicPartition, Long> offsets) {
        Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> kafkaOffsets = offsets.entrySet().stream()
                .collect(Collectors.toMap(e -> new org.apache.kafka.common.TopicPartition(e.getKey().topic(), e.getKey().partition()),
                        e -> new OffsetAndMetadata(e.getValue())));
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.alterConsumerGroupOffsets(groupId, kafkaOffsets).all().get();
        }
        catch (ExecutionException | KafkaException e) {
            throw new KafkaClientException("Failed to commit offsets for group '" + groupId + "'", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaClientException("Interrupted committing offsets for group '" + groupId + "'", e);
        }
    }

    private static Map<String, Object> producerConfig(String bootstrap, Map<String, String> config) {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerConfig.putAll(config);
        return producerConfig;
    }

    private static ProducerRecord<String, String> toProducerRecord(String topic, ProduceRecord record) {
        var producerRecord = new ProducerRecord<>(topic, record.key(), record.value());
        record.headers().forEach((k, v) -> producerRecord.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        return producerRecord;
    }

    private static Map<String, String> headersOf(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }
}
