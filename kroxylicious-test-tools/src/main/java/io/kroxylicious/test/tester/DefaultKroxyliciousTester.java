/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.KroxyliciousConfig;
import io.kroxylicious.proxy.KroxyliciousConfigBuilder;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.test.client.KafkaClient;

public class DefaultKroxyliciousTester implements KroxyliciousTester {
    private final AutoCloseable proxy;
    private final KroxyliciousConfig kroxyliciousConfig;

    private final KroxyliciousClients defaultClients;

    DefaultKroxyliciousTester(KroxyliciousConfigBuilder configurationBuilder) {
        this(configurationBuilder, config -> {
            KafkaProxy kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config.toYaml()));
            try {
                kafkaProxy.startup();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return kafkaProxy;
        });
    }

    DefaultKroxyliciousTester(KroxyliciousConfigBuilder configuration, Function<KroxyliciousConfig, AutoCloseable> kroxyliciousFactory) {
        kroxyliciousConfig = configuration.build();
        proxy = kroxyliciousFactory.apply(kroxyliciousConfig);
        int numVirtualClusters = kroxyliciousConfig.getVirtualClusters().size();
        if (numVirtualClusters == 1) {
            String onlyCluster = kroxyliciousConfig.getVirtualClusters().keySet().stream().findFirst().orElseThrow();
            defaultClients = new KroxyliciousClients(KroxyliciousConfigUtils.bootstrapServersFor(onlyCluster, kroxyliciousConfig));
        }
        else {
            defaultClients = null;
        }
    }

    private KroxyliciousClients clients() {
        int numVirtualClusters = kroxyliciousConfig.getVirtualClusters().size();
        if (numVirtualClusters == 1) {
            String onlyCluster = kroxyliciousConfig.getVirtualClusters().keySet().stream().findFirst().orElseThrow();
            return new KroxyliciousClients(KroxyliciousConfigUtils.bootstrapServersFor(onlyCluster, kroxyliciousConfig));
        }
        else {
            throw new AmbiguousVirtualClusterException(
                    "no default virtual cluster determined because there were multiple or no virtual clusters in kroxylicious configuration");
        }
    }

    private KroxyliciousClients clients(String virtualCluster) {
        return new KroxyliciousClients(KroxyliciousConfigUtils.bootstrapServersFor(virtualCluster, kroxyliciousConfig));
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
    public KafkaClient singleRequestClient() {
        return clients().singleRequestClient();
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

    @Override
    public KafkaClient singleRequestClient(String virtualCluster) {
        return clients(virtualCluster).singleRequestClient();
    }

    @Override
    public void close() {
        try {
            proxy.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
