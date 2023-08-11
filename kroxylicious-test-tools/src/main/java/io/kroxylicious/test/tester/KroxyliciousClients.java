/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.HashMap;
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

    KroxyliciousClients(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Admin admin(Map<String, Object> additionalConfig) {
        Map<String, Object> config = createConfigMap(bootstrapServers, additionalConfig);
        return CloseableAdmin.create(config);
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
        return CloseableProducer.wrap(new KafkaProducer<>(config, keySerde.serializer(), valueSerde.serializer()));
    }

    public Consumer<String, String> consumer(Map<String, Object> additionalConfig) {
        return consumer(Serdes.String(), Serdes.String(), additionalConfig);
    }

    public Consumer<String, String> consumer() {
        return consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    public <U, V> Consumer<U, V> consumer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        Map<String, Object> config = createConfigMap(bootstrapServers, additionalConfig);
        return CloseableConsumer.wrap(new KafkaConsumer<>(config, keySerde.deserializer(), valueSerde.deserializer()));
    }

    public KafkaClient simpleTestClient() {
        String[] hostPort = bootstrapServers.split(":");
        return new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]));
    }

    private Map<String, Object> createConfigMap(String bootstrapServers, Map<String, Object> additionalConfig) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(additionalConfig);
        return config;
    }

}
