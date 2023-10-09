/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.test.client.KafkaClient;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

public class DefaultKroxyliciousTester implements KroxyliciousTester {
    private AutoCloseable proxy;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<KroxyliciousTesterBuilder.TrustStoreConfiguration> trustStoreConfiguration;

    private final Configuration kroxyliciousConfig;

    private final Map<String, KroxyliciousClients> clients;
    private final Map<String, Set<String>> topicsPerVirtialCluster;

    private final ClientFactory clientFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKroxyliciousTester.class);

    DefaultKroxyliciousTester(ConfigurationBuilder configurationBuilder, Function<Configuration, AutoCloseable> kroxyliciousFactory, ClientFactory clientFactory,
                              @Nullable KroxyliciousTesterBuilder.TrustStoreConfiguration trustStoreConfiguration) {
        this.kroxyliciousConfig = configurationBuilder.build();
        this.proxy = kroxyliciousFactory.apply(kroxyliciousConfig);
        this.trustStoreConfiguration = Optional.ofNullable(trustStoreConfiguration);
        this.clients = new ConcurrentHashMap<>();
        this.clientFactory = clientFactory;
        topicsPerVirtialCluster = new ConcurrentHashMap<>();
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
        return clients.computeIfAbsent(virtualCluster,
                k -> clientFactory.build(virtualCluster, buildDefaultClientConfiguration(virtualCluster)));
    }

    @NonNull
    private Map<String, Object> buildDefaultClientConfiguration(String virtualCluster) {
        Map<String, Object> defaultClientConfig = new HashMap<>();
        defaultClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KroxyliciousConfigUtils.bootstrapServersFor(virtualCluster, kroxyliciousConfig));
        configureClientTls(virtualCluster, defaultClientConfig);
        return defaultClientConfig;
    }

    private void configureClientTls(String virtualCluster, Map<String, Object> defaultClientConfig) {
        final VirtualCluster definedCluster = kroxyliciousConfig.virtualClusters().get(virtualCluster);
        if (definedCluster != null) {
            final Optional<Tls> tls = definedCluster.tls();
            if (tls.isPresent() && trustStoreConfiguration.isPresent()) {
                final KroxyliciousTesterBuilder.TrustStoreConfiguration storeConfiguration = trustStoreConfiguration.get();
                defaultClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
                defaultClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, storeConfiguration.trustStoreLocation());
                defaultClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, storeConfiguration.trustStorePassword());
            }
            // Technically tls present and possibleKeytoolCertificateGenerator being empty is an error condition. But debatable if we should prevent that here
        }
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

    static KafkaProxy spawnProxy(Configuration config) {
        KafkaProxy kafkaProxy = new KafkaProxy(config);
        try {
            kafkaProxy.startup();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return kafkaProxy;
    }

    @Override
    public Set<String> createTopics(String virtualCluster, int numberOfTopics) {
        try (Admin admin = clients(virtualCluster).admin()) {
            final List<NewTopic> newTopics = IntStream.range(0, numberOfTopics).mapToObj(ignored -> {
                final String topicName = MobyNamesGenerator.getRandomName();
                return new NewTopic(topicName, (short) 1, (short) 1);
            }).toList();
            final CreateTopicsResult createTopicsResult = admin.createTopics(newTopics);
            createTopicsResult.all().get(30, TimeUnit.SECONDS);

            // TODO should this be driven by result of the createTopic call?
            final Set<String> topicNames = newTopics.stream().map(NewTopic::name).collect(Collectors.toSet());
            topicsForVirtualCluster(virtualCluster).addAll(topicNames);
            return topicNames;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("failed to create topics on " + virtualCluster, e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("failed to create topics on " + virtualCluster, e);
        }
    }

    @Override
    public void deleteTopics(String virtualCluster) {
        try (Admin admin = clients(virtualCluster).admin()) {
            final Set<String> topics = topicsForVirtualCluster(virtualCluster);
            if (!topics.isEmpty()) {
                admin.deleteTopics(topics)
                        .all()
                        .get(30, TimeUnit.SECONDS);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("failed to delete topics on " + virtualCluster, e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("failed to delete topics on " + virtualCluster, e);
        }
    }

    private Set<String> topicsForVirtualCluster(String clusterName) {
        return topicsPerVirtialCluster.computeIfAbsent(clusterName, key -> new HashSet<>());
    }

    @FunctionalInterface
    interface ClientFactory {
        KroxyliciousClients build(String clusterName, Map<String, Object> defaultClientConfig);
    }

}
