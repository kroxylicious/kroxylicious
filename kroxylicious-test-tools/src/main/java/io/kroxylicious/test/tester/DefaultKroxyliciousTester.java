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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.test.client.KafkaClient;

public class DefaultKroxyliciousTester implements KroxyliciousTester {
    private AutoCloseable proxy;
    private final Configuration kroxyliciousConfig;

    private final Map<String, KroxyliciousClients> clients;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKroxyliciousTester.class);

    DefaultKroxyliciousTester(ConfigurationBuilder configurationBuilder) {
        this(configurationBuilder, DefaultKroxyliciousTester::spawnProxy);
    }

    DefaultKroxyliciousTester(ConfigurationBuilder configuration, Function<Configuration, AutoCloseable> kroxyliciousFactory) {
        kroxyliciousConfig = configuration.build();
        proxy = kroxyliciousFactory.apply(kroxyliciousConfig);
        clients = new HashMap<>();
    }

    private KroxyliciousClients clients() {
        int numVirtualClusters = kroxyliciousConfig.virtualClusters().size();
        if (numVirtualClusters == 1) {
            String onlyCluster = kroxyliciousConfig.virtualClusters().keySet().stream().findFirst().orElseThrow();
            return clients(onlyCluster);
        }
        else {
            throw new AmbiguousVirtualClusterException(
                    "no default virtual cluster determined because there were multiple or no virtual clusters in kroxylicious configuration");
        }
    }

    private KroxyliciousClients clients(String virtualCluster) {
        return clients.computeIfAbsent(virtualCluster, k -> new KroxyliciousClients(KroxyliciousConfigUtils.bootstrapServersFor(k, kroxyliciousConfig)));
    }

    @Override
    public Admin admin(Map<String, Object> additionalConfig) {
        return clients().admin(additionalConfig);
    }

    @Override
    public Admin admin() {
        return clients().admin();
    }

    @Override
    public Producer<String, String> producer(Map<String, Object> additionalConfig) {
        return clients().producer(additionalConfig);
    }

    @Override
    public Producer<String, String> producer() {
        return clients().producer();
    }

    @Override
    public <U, V> Producer<U, V> producer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        return clients().producer(keySerde, valueSerde, additionalConfig);
    }

    @Override
    public Consumer<String, String> consumer(Map<String, Object> additionalConfig) {
        return clients().consumer(additionalConfig);
    }

    @Override
    public Consumer<String, String> consumer() {
        return clients().consumer();
    }

    @Override
    public <U, V> Consumer<U, V> consumer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        return clients().consumer(keySerde, valueSerde, additionalConfig);
    }

    @Override
    public KafkaClient simpleTestClient() {
        return clients().simpleTestClient();
    }

    @Override
    public KafkaClient simpleTestClient(String virtualCluster) {
        return clients(virtualCluster).simpleTestClient();
    }

    @Override
    public Admin admin(String virtualCluster, Map<String, Object> additionalConfig) {
        return clients(virtualCluster).admin(additionalConfig);
    }

    @Override
    public Admin admin(String virtualCluster) {
        return clients(virtualCluster).admin();
    }

    @Override
    public Producer<String, String> producer(String virtualCluster, Map<String, Object> additionalConfig) {
        return clients(virtualCluster).producer(additionalConfig);
    }

    @Override
    public Producer<String, String> producer(String virtualCluster) {
        return clients(virtualCluster).producer();
    }

    @Override
    public <U, V> Producer<U, V> producer(String virtualCluster, Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        return clients(virtualCluster).producer(keySerde, valueSerde, additionalConfig);
    }

    @Override
    public Consumer<String, String> consumer(String virtualCluster, Map<String, Object> additionalConfig) {
        return clients(virtualCluster).consumer(additionalConfig);
    }

    @Override
    public Consumer<String, String> consumer(String virtualCluster) {
        return clients(virtualCluster).consumer();
    }

    @Override
    public <U, V> Consumer<U, V> consumer(String virtualCluster, Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        return clients(virtualCluster).consumer(keySerde, valueSerde, additionalConfig);
    }

    public CreateTopicsResult createTopics(String virtualCluster, Map<String, Object> additionalConfig, NewTopic... topics) {
        try (var admin = admin(virtualCluster, additionalConfig)) {
            return createTopicsWithAdmin(admin, topics);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public CreateTopicsResult createTopic(String virtualCluster, Map<String, Object> additionalConfig, String topic, int numPartitions) {
        return createTopics(virtualCluster, additionalConfig, new NewTopic(topic, numPartitions, (short) 1));
    }

    public CreateTopicsResult createTopic(String virtualCluster, String topic, int numPartitions) {
        return createTopics(virtualCluster, Map.of(), new NewTopic(topic, numPartitions, (short) 1));
    }

    public CreateTopicsResult createTopics(NewTopic... topics) {
        try (var admin = admin()) {
            return createTopicsWithAdmin(admin, topics);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public CreateTopicsResult createTopic(String topic, int numPartitions) {
        return createTopics(new NewTopic(topic, numPartitions, (short) 1));
    }

    private CreateTopicsResult createTopicsWithAdmin(Admin admin, NewTopic... topics) throws ExecutionException, InterruptedException, TimeoutException {
        List<NewTopic> topicsList = List.of(topics);
        var created = admin.createTopics(topicsList);
        created.all().get(10, TimeUnit.SECONDS);
        return created;
    }

    public DeleteTopicsResult deleteTopics(String virtualCluster, Map<String, Object> additionalConfig, TopicCollection topics) {
        try (var admin = admin(virtualCluster, additionalConfig)) {
            return deleteTopicsWithAdmin(admin, topics);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public DeleteTopicsResult deleteTopics(String virtualCluster, TopicCollection topics) {
        return deleteTopics(virtualCluster, Map.of(), topics);
    }

    public DeleteTopicsResult deleteTopics(TopicCollection topics) {
        try (var admin = admin()) {
            return deleteTopicsWithAdmin(admin, topics);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private DeleteTopicsResult deleteTopicsWithAdmin(Admin admin, TopicCollection topics) throws ExecutionException, InterruptedException, TimeoutException {
        var deleted = admin.deleteTopics(topics);
        deleted.all().get(10, TimeUnit.SECONDS);
        return deleted;
    }

    public void restartProxy() {
        try {
            proxy.close();
            proxy = spawnProxy(kroxyliciousConfig);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            List<Exception> exceptions = new ArrayList<>();
            for (KroxyliciousClients c : clients.values()) {
                try {
                    c.close();
                }
                catch (Exception e) {
                    exceptions.add(e);
                }
            }
            proxy.close();
            if (!exceptions.isEmpty()) {
                // if we encountered any exceptions while closing, log them all and then throw whichever one came first.
                exceptions.forEach(e -> LOGGER.error(e.getMessage(), e));
                throw exceptions.get(0);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static KafkaProxy spawnProxy(Configuration config) {
        KafkaProxy kafkaProxy = new KafkaProxy(config);
        try {
            kafkaProxy.startup();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return kafkaProxy;
    }

}
