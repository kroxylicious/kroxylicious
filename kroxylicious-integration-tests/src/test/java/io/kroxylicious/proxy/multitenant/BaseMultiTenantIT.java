/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.multitenant;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterListenerBuilder;
import io.kroxylicious.proxy.filter.multitenant.MultiTenant;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(KafkaClusterExtension.class)
public abstract class BaseMultiTenantIT extends BaseIT {

    public static final String TENANT_1_CLUSTER = "foo";
    static final HostPort TENANT_1_PROXY_ADDRESS = HostPort
            .parse(IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(TENANT_1_CLUSTER, 9192));
    public static final String TENANT_2_CLUSTER = "bar";
    static final HostPort TENANT_2_PROXY_ADDRESS = HostPort
            .parse(IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(TENANT_2_CLUSTER, 9292));

    static final long FUTURE_TIMEOUT_SECONDS = 5L;
    Map<String, Object> clientConfig;

    TestInfo testInfo;
    KeytoolCertificateGenerator certificateGenerator;
    @TempDir
    Path certsDirectory;

    @BeforeEach
    void beforeEach(TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        // TODO: use a per-tenant server certificate.
        this.certificateGenerator = new KeytoolCertificateGenerator();
        this.certificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("*"),
                "KI", "RedHat", null, null, "US");
        Path clientTrustStore = certsDirectory.resolve(certificateGenerator.getTrustStoreLocation());
        this.certificateGenerator.generateTrustStore(this.certificateGenerator.getCertFilePath(), "client", clientTrustStore.toAbsolutePath().toString());
        this.clientConfig = Map.of(CommonClientConfigs.CLIENT_ID_CONFIG, testInfo.getDisplayName(),
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, certificateGenerator.getPassword());
    }

    static ConfigurationBuilder getConfig(KafkaCluster cluster, KeytoolCertificateGenerator certificateGenerator) {
        return getConfig(cluster, certificateGenerator, (Map<String, Object>) null);
    }

    static ConfigurationBuilder getConfig(KafkaCluster cluster, KeytoolCertificateGenerator certificateGenerator, Map<String, Object> filterConfig) {
        var filterBuilder = new NamedFilterDefinitionBuilder("filter-1", MultiTenant.class.getName());
        Optional.ofNullable(filterConfig).ifPresent(filterBuilder::withConfig);
        return getConfig(cluster, certificateGenerator, filterBuilder);
    }

    static ConfigurationBuilder getConfig(KafkaCluster cluster, KeytoolCertificateGenerator certificateGenerator, NamedFilterDefinitionBuilder filterBuilder) {
        return new ConfigurationBuilder()
                .addToVirtualClusters(TENANT_1_CLUSTER, new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .addToListeners("default", new VirtualClusterListenerBuilder()
                                .withClusterNetworkAddressConfigProvider(
                                        new ClusterNetworkAddressConfigProviderDefinitionBuilder(PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                                                .withConfig("bootstrapAddress", TENANT_1_PROXY_ADDRESS)
                                                .build())
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(certificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(certificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .endTls()
                                .build())
                        .build())
                .addToVirtualClusters(TENANT_2_CLUSTER, new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .addToListeners("default", new VirtualClusterListenerBuilder()
                                .withClusterNetworkAddressConfigProvider(
                                        new ClusterNetworkAddressConfigProviderDefinitionBuilder(PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                                                .withConfig("bootstrapAddress", TENANT_2_PROXY_ADDRESS)
                                                .build())
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(certificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(certificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .endTls()
                                .build())
                        .build())
                .addToFilterDefinitions(filterBuilder.build())
                .addToDefaultFilters(filterBuilder.name());
    }

    Consumer<String, String> getConsumerWithConfig(KroxyliciousTester tester, String virtualCluster, String groupId, Map<String, Object> baseConfig,
                                                   Map<String, Object> additionalConfig) {
        Map<String, Object> standardConfig = Map.of(ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.FALSE.toString(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        return getConsumerWithConfig(tester, Optional.of(virtualCluster), baseConfig, standardConfig, additionalConfig);
    }

    void consumeAndAssert(KroxyliciousTester tester, Map<String, Object> clientConfig, String virtualCluster, String topicName, String groupId,
                          Deque<Predicate<ConsumerRecord<String, String>>> expected, boolean offsetCommit) {
        try (var consumer = getConsumerWithConfig(tester, virtualCluster, groupId, clientConfig, Map.of(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.format("%d", expected.size())))) {

            var topicPartitions = List.of(new TopicPartition(topicName, 0));
            consumer.assign(topicPartitions);

            while (!expected.isEmpty()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
                assertThat(records.partitions()).hasSizeGreaterThanOrEqualTo(1);
                records.forEach(r -> {
                    assertThat(expected).withFailMessage("received unexpected record %s", r).isNotEmpty();
                    var predicate = expected.pop();
                    assertThat(r).matches(predicate, predicate.toString());
                });
            }

            if (offsetCommit) {
                consumer.commitSync(Duration.ofSeconds(5));
            }
        }
    }

    void produceAndAssert(KroxyliciousTester tester, Map<String, Object> clientConfig, String virtualCluster,
                          Stream<ProducerRecord<String, String>> records, Optional<String> transactionalId) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000);
        transactionalId.ifPresent(tid -> config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId.get()));
        try (var producer = getProducerWithConfig(tester, Optional.of(virtualCluster), clientConfig, config)) {
            transactionalId.ifPresent(u -> {
                producer.initTransactions();
                producer.beginTransaction();
            });

            records.forEach(rec -> {
                RecordMetadata recordMetadata = null;
                try {
                    recordMetadata = producer.send(rec).get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                catch (Exception e) {
                    fail("Caught: %s producing to %s", e.getMessage(), rec.topic());
                }
                assertThat(recordMetadata).isNotNull();
                assertThat(rec.topic()).isNotNull();
                assertThat(recordMetadata.topic()).isNotNull();
            });

            transactionalId.ifPresent(u -> {
                producer.commitTransaction();
            });

        }
    }

    @NonNull
    static <T, V> Condition<T> matches(Function<T, V> extractor, V expectedValue) {
        return new Condition<>(item -> Objects.equals(extractor.apply(item), expectedValue), "unexpected entry");
    }

    @NonNull
    static <K, V> Predicate<ConsumerRecord<K, V>> matchesRecord(final String expectedTopic, final K expectedKey, final V expectedValue) {
        return new Predicate<>() {
            @Override
            public boolean test(ConsumerRecord<K, V> item) {
                return Objects.equals(item.topic(), expectedTopic) && Objects.equals(item.key(), expectedKey) && Objects.equals(
                        item.value(), expectedValue);
            }

            @Override
            public String toString() {
                return String.format("expected: key %s value %s", expectedKey, expectedValue);
            }
        };
    }
}
