/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;

class KroxyliciousClients implements Closeable {
    private final Map<String, Object> defaultClientConfiguration;

    private final List<Admin> admins;
    private final List<Producer<?, ?>> producers;
    private final List<Consumer<?, ?>> consumers;
    private final ClientFactory clientFactory;

    KroxyliciousClients(Map<String, Object> defaultClientConfiguration) {
        this(defaultClientConfiguration, new ClientFactory() {
        });
    }

    KroxyliciousClients(Map<String, Object> defaultClientConfiguration, ClientFactory clientFactory) {
        this.defaultClientConfiguration = defaultClientConfiguration;
        this.admins = new ArrayList<>();
        this.producers = new ArrayList<>();
        this.consumers = new ArrayList<>();
        this.clientFactory = clientFactory;
    }

    public Admin admin(Map<String, Object> additionalConfig) {
        Map<String, Object> config = createClientConfig(additionalConfig);
        Admin admin = clientFactory.newAdmin(config);
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
        Map<String, Object> config = createClientConfig(additionalConfig);
        Producer<U, V> producer = this.clientFactory.newProducer(config, keySerde.serializer(), valueSerde.serializer());
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
        Map<String, Object> config = createClientConfig(additionalConfig);
        Consumer<U, V> consumer = clientFactory.newConsumer(config, keySerde.deserializer(), valueSerde.deserializer());
        consumers.add(consumer);
        return consumer;
    }

    public KafkaClient simpleTestClient() {
        String[] hostPort = defaultClientConfiguration.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString().split(":");
        return new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]));
    }

    public void close() {
        List<Exception> exceptions = new ArrayList<>();
        try {
            exceptions.addAll(batchClose(admins));
            exceptions.addAll(batchClose(producers));
            exceptions.addAll(batchClose(consumers));
            if (!exceptions.isEmpty()) {
                // if we encountered any exceptions while closing, throw whichever one came first.
                throw exceptions.get(0);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Collection<Exception> batchClose(Collection<? extends AutoCloseable> closeables) {
        List<Exception> exceptions = new ArrayList<>();
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            }
            catch (Exception e) {
                exceptions.add(e);
            }
        }
        return exceptions;
    }

    private Map<String, Object> createClientConfig(Map<String, Object> additionalConfig) {
        Map<String, Object> config = new HashMap<>(defaultClientConfiguration);
        config.putAll(additionalConfig);
        return config;
    }

    interface ClientFactory {
        default Admin newAdmin(Map<String, Object> clientConfiguration) {
            return CloseableAdmin.wrap(Admin.create(clientConfiguration));
        }

        default <K, V> Consumer<K, V> newConsumer(Map<String, Object> clientConfiguration, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
            return CloseableConsumer.wrap(new KafkaConsumer<>(clientConfiguration, keyDeserializer, valueDeserializer));
        }

        default <K, V> Producer<K, V> newProducer(Map<String, Object> clientConfiguration, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            return CloseableProducer.wrap(new KafkaProducer<>(clientConfiguration, keySerializer, valueSerializer));
        }
    }
}
