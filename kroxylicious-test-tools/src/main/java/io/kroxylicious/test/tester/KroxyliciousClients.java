/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;

class KroxyliciousClients {
    private final String bootstrapServers;

    private final List<Admin> admins;
    private final List<Producer> producers;
    private final List<Consumer> consumers;

    KroxyliciousClients(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.admins = new ArrayList<>();
        this.producers = new ArrayList<>();
        this.consumers = new ArrayList<>();
    }

    public Admin admin(Map<String, Object> additionalConfig) {
        Map<String, Object> config = createConfigMap(bootstrapServers, additionalConfig);
        Admin admin = CloseableAdmin.create(config);
        admins.add(admin);
        return admin;
    }

    public Admin admin() {
        return admin(Map.of());
    }

    public Producer<String, String> producer(Map<String, Object> additionalConfig) {
        return producer(Serdes.String(), Serdes.String(), additionalConfig);
    }

    public Producer<String, String> producer() {
        return producer(Map.of());
    }

    public <U, V> Producer<U, V> producer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        Map<String, Object> config = createConfigMap(bootstrapServers, additionalConfig);
        Producer<U, V> producer = CloseableProducer.wrap(new KafkaProducer<>(config, keySerde.serializer(), valueSerde.serializer()));
        producers.add(producer);
        return producer;
    }

    public Consumer<String, String> consumer(Map<String, Object> additionalConfig) {
        return consumer(Serdes.String(), Serdes.String(), additionalConfig);
    }

    public Consumer<String, String> consumer() {
        return consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    public <U, V> Consumer<U, V> consumer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        Map<String, Object> config = createConfigMap(bootstrapServers, additionalConfig);
        Consumer<U, V> consumer = CloseableConsumer.wrap(new KafkaConsumer<>(config, keySerde.deserializer(), valueSerde.deserializer()));
        consumers.add(consumer);
        return consumer;
    }

    public KafkaClient simpleTestClient() {
        String[] hostPort = bootstrapServers.split(":");
        return new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]));
    }

    public void close() {
        try {
            admins.forEach(Admin::close);
            producers.forEach(Producer::close);
            consumers.forEach(Consumer::close);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> createConfigMap(String bootstrapServers, Map<String, Object> additionalConfig) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(additionalConfig);
        return config;
    }

}
