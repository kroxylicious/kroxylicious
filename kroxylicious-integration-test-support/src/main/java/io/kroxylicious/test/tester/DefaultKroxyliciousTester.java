/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.test.client.KafkaClient;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_GATEWAY_NAME;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.getVirtualClusterGatewayStream;

public class DefaultKroxyliciousTester implements KroxyliciousTester {
    private AutoCloseable proxy;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<KroxyliciousTesterBuilder.TrustStoreConfiguration> trustStoreConfiguration;

    private final Configuration kroxyliciousConfig;

    private final Map<GatewayId, KroxyliciousClients> clients;
    private final Map<String, Set<String>> topicsPerVirtualCluster;

    private final ClientFactory clientFactory;

    private final List<Closeable> closeables = new ArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKroxyliciousTester.class);

    DefaultKroxyliciousTester(ConfigurationBuilder configurationBuilder, Function<Configuration, AutoCloseable> kroxyliciousFactory, ClientFactory clientFactory,
                              @Nullable KroxyliciousTesterBuilder.TrustStoreConfiguration trustStoreConfiguration) {
        this.kroxyliciousConfig = configurationBuilder.build();
        this.proxy = kroxyliciousFactory.apply(kroxyliciousConfig);
        this.trustStoreConfiguration = Optional.ofNullable(trustStoreConfiguration);
        this.clients = new ConcurrentHashMap<>();
        this.clientFactory = clientFactory;
        topicsPerVirtualCluster = new ConcurrentHashMap<>();
    }

    private KroxyliciousClients clients() {
        return clients(onlyVirtualCluster(), DEFAULT_GATEWAY_NAME);
    }

    private String onlyVirtualCluster() {
        int numVirtualClusters = kroxyliciousConfig.virtualClusters().size();
        if (numVirtualClusters == 1) {
            return kroxyliciousConfig.virtualClusters().keySet().stream().findFirst().orElseThrow();
        }
        else {
            throw new AmbiguousVirtualClusterException(
                    "no default virtual cluster determined because there were multiple or no virtual clusters in kroxylicious configuration");
        }
    }

    private KroxyliciousClients clients(String virtualCluster, String gateway) {
        GatewayId key = new GatewayId(virtualCluster, gateway);
        return clients.computeIfAbsent(key,
                k -> clientFactory.build(key, buildDefaultClientConfiguration(virtualCluster, gateway)));
    }

    @NonNull
    private Map<String, Object> buildDefaultClientConfiguration(String virtualCluster, String gateway) {
        Map<String, Object> defaultClientConfig = new HashMap<>();
        defaultClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapAddress(virtualCluster, gateway));
        configureClientTls(virtualCluster, defaultClientConfig, gateway);
        return defaultClientConfig;
    }

    @Override
    @NonNull
    public String getBootstrapAddress() {
        return getBootstrapAddress(onlyVirtualCluster(), DEFAULT_GATEWAY_NAME);
    }

    @Override
    @NonNull
    public String getBootstrapAddress(String virtualCluster, String gateway) {
        return KroxyliciousConfigUtils.bootstrapServersFor(virtualCluster, kroxyliciousConfig, gateway);
    }

    private void configureClientTls(String virtualCluster, Map<String, Object> defaultClientConfig, String gateway) {
        final VirtualCluster definedCluster = kroxyliciousConfig.virtualClusters().get(virtualCluster);
        if (definedCluster != null) {

            var first = getVirtualClusterGatewayStream(definedCluster).filter(g -> g.name().equals(gateway)).findFirst();
            var vcl = first.orElseThrow(() -> new IllegalArgumentException("cluster " + virtualCluster + " does not contain gateway named " + gateway));
            final Optional<Tls> tls = vcl.tls();
            if (tls.isPresent()) {
                defaultClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
                if (trustStoreConfiguration.isPresent()) {
                    final KroxyliciousTesterBuilder.TrustStoreConfiguration storeConfiguration = trustStoreConfiguration.get();
                    defaultClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, storeConfiguration.trustStoreLocation());
                    defaultClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, storeConfiguration.trustStorePassword());
                }
            }
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
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).simpleTestClient();
    }

    @Override
    public Admin admin(String virtualCluster, Map<String, Object> additionalConfig) {
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).admin(additionalConfig);
    }

    @Override
    public Admin admin(String virtualCluster, String gatewayName, Map<String, Object> additionalConfig) {
        return clients(virtualCluster, gatewayName).admin(additionalConfig);
    }

    @Override
    public Admin admin(String virtualCluster) {
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).admin();
    }

    @Override
    public Admin admin(String virtualCluster, String gateway) {
        return clients(virtualCluster, gateway).admin();
    }

    @Override
    public Producer<String, String> producer(String virtualCluster, Map<String, Object> additionalConfig) {
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).producer(additionalConfig);
    }

    @Override
    public Producer<String, String> producer(String virtualCluster) {
        return producer(virtualCluster, DEFAULT_GATEWAY_NAME);
    }

    @Override
    public Producer<String, String> producer(String virtualCluster, String gateway) {
        return clients(virtualCluster, gateway).producer();
    }

    @Override
    public <U, V> Producer<U, V> producer(String virtualCluster, Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).producer(keySerde, valueSerde, additionalConfig);
    }

    @Override
    public Consumer<String, String> consumer(String virtualCluster, Map<String, Object> additionalConfig) {
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).consumer(additionalConfig);
    }

    @Override
    public Consumer<String, String> consumer(String virtualCluster) {
        return consumer(virtualCluster, DEFAULT_GATEWAY_NAME);
    }

    @Override
    public Consumer<String, String> consumer(String virtualCluster, String gateway) {
        return clients(virtualCluster, gateway).consumer();
    }

    @Override
    public <U, V> Consumer<U, V> consumer(String virtualCluster, Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig) {
        return clients(virtualCluster, DEFAULT_GATEWAY_NAME).consumer(keySerde, valueSerde, additionalConfig);
    }

    public void restartProxy() {
        try {
            proxy.close();
            proxy = spawnProxy(kroxyliciousConfig, Features.defaultFeatures());
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        try {
            List<Exception> exceptions = new ArrayList<>();
            for (KroxyliciousClients c : clients.values()) {
                closeCloseable(c).ifPresent(exceptions::add);
            }
            closeables.forEach(c -> closeCloseable(c).ifPresent(exceptions::add));
            proxy.close();
            if (!exceptions.isEmpty()) {
                // if we encountered any exceptions while closing, log them all and then throw whichever one came first.
                exceptions.forEach(e -> LOGGER.error(e.getMessage(), e));
                throw exceptions.get(0);
            }
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Optional<Exception> closeCloseable(Closeable c) {
        try {
            c.close();
            return Optional.empty();
        }
        catch (Exception e) {
            return Optional.of(e);
        }
    }

    static KafkaProxy spawnProxy(Configuration config, Features features) {
        KafkaProxy kafkaProxy = new KafkaProxy(new ServiceBasedPluginFactoryRegistry(), config, features);
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
        try (Admin admin = clients(virtualCluster, DEFAULT_GATEWAY_NAME).admin()) {
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
            throw new IllegalStateException("Failed to create topics on " + virtualCluster, e);
        }
        catch (TimeoutException e) {
            throw new IllegalStateException("Timed out creating topics on " + virtualCluster, e);
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Failed to create topics on " + virtualCluster, e.getCause());
        }
    }

    @Override
    public void deleteTopics(String virtualCluster) {
        try (Admin admin = clients(virtualCluster, DEFAULT_GATEWAY_NAME).admin()) {
            final Set<String> topics = topicsForVirtualCluster(virtualCluster);
            if (!topics.isEmpty()) {
                admin.deleteTopics(topics)
                        .all()
                        .get(30, TimeUnit.SECONDS);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to delete topics on " + virtualCluster, e);
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Failed to delete topics on " + virtualCluster, e.getCause());
        }
        catch (TimeoutException e) {
            throw new IllegalStateException("Timed out deleting topics on " + virtualCluster, e);
        }
    }

    @Override
    public AdminHttpClient getAdminHttpClient() {
        var client = Optional.ofNullable(kroxyliciousConfig.adminHttpConfig())
                .map(ahc -> URI.create("http://localhost:" + ahc.port()))
                .map(AdminHttpClient::new)
                .orElseThrow(() -> new IllegalStateException("admin http interface not configured"));
        closeables.add(client);
        return client;
    }

    private Set<String> topicsForVirtualCluster(String clusterName) {
        return topicsPerVirtualCluster.computeIfAbsent(clusterName, key -> ConcurrentHashMap.newKeySet());
    }

    @FunctionalInterface
    interface ClientFactory {
        KroxyliciousClients build(GatewayId gateway, Map<String, Object> defaultClientConfig);
    }

}
