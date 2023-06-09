/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Integration tests that focus on how Kroxylicious presents itself to the network.
 *
 * TODO corner case test - verify kroxy's ability to recover for a temporary port already bound condition.
 */
@ExtendWith(KafkaClusterExtension.class)
public class ExpositionIT {

    private static final String TOPIC = "my-test-topic";
    public static final HostPort PROXY_ADDRESS = HostPort.parse("localhost:9192");

    @TempDir
    private Path certsDirectory;

    private record ClientTrust(Path trustStore, String password) {
    }

    @Test
    public void exposesSingleUpstreamClusterOverTls(KafkaCluster cluster) throws Exception {

        var brokerCertificateGenerator = new KeytoolCertificateGenerator();
        brokerCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "localhost", "KI", "RedHat", null, null, "US");
        var clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        brokerCertificateGenerator.generateTrustStore(brokerCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        String bootstrapServers = cluster.getBootstrapServers();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", PROXY_ADDRESS)
                                        .build())
                        .build())
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        var demo = builder.getVirtualClusters().get("demo");
        demo = new VirtualClusterBuilder(demo)
                .withNewTls()
                .withNewKeyStoreKey()
                .withStoreFile(brokerCertificateGenerator.getKeyStoreLocation())
                .withNewInlinePasswordStore(brokerCertificateGenerator.getPassword())
                .endKeyStoreKey()
                .endTls()
                .build();
        builder.addToVirtualClusters("demo", demo);

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo", Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, brokerCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            createTopic(admin, TOPIC, 1);

            var connectionsMetric = admin.metrics().entrySet().stream().filter(metricNameEntry -> "connections".equals(metricNameEntry.getKey().name()))
                    .findFirst();
            assertTrue(connectionsMetric.isPresent());
            var protocol = connectionsMetric.get().getKey().tags().get("protocol");
            assertThat(protocol).startsWith("TLS");
        }
    }

    @Test
    public void exposesTwoClusterOverPlainWithSeparatePorts(KafkaCluster cluster) {
        List<String> clusterProxyAddresses = List.of("localhost:9192", "localhost:9294");

        var builder = new ConfigurationBuilder()
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        var base = new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .build();

        for (int i = 0; i < clusterProxyAddresses.size(); i++) {
            var bootstrap = clusterProxyAddresses.get(i);
            var virtualCluster = new VirtualClusterBuilder(base)
                    .withClusterNetworkAddressConfigProvider(
                            new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", bootstrap)
                                    .build())
                    .build();
            builder.addToVirtualClusters("cluster" + i, virtualCluster);

        }

        try (var tester = kroxyliciousTester(builder)) {
            for (int i = 0; i < clusterProxyAddresses.size(); i++) {
                try (var admin = tester.admin("cluster" + i)) {
                    // do some work to ensure virtual cluster is operational
                    createTopic(admin, TOPIC + i, 1);
                }
            }
        }
    }

    @Test
    public void exposesTwoSeparateUpstreamClustersUsingSniRouting(KafkaCluster cluster) throws Exception {
        var clientTrust = new ArrayList<ClientTrust>();
        var virtualClusterCommonNamePattern = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".virtualcluster%d");
        var virtualClusterBootstrapPattern = "bootstrap" + virtualClusterCommonNamePattern;
        var virtualClusterBrokerAddressPattern = "broker-$(nodeId)" + virtualClusterCommonNamePattern;

        var builder = new ConfigurationBuilder()
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        var base = new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .build();

        int numberOfVirtualClusters = 2;
        for (int i = 0; i < numberOfVirtualClusters; i++) {

            var virtualClusterFQDN = virtualClusterBootstrapPattern.formatted(i);
            var brokerCertificateGenerator = new KeytoolCertificateGenerator();
            brokerCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "*" + virtualClusterCommonNamePattern.formatted(i), "KI", "RedHat", null,
                    null, "US");
            var clientTrustStore = certsDirectory.resolve("demo-%d.truststore.jks".formatted(i));
            brokerCertificateGenerator.generateTrustStore(brokerCertificateGenerator.getCertFilePath(), "client", clientTrustStore.toAbsolutePath().toString());
            clientTrust.add(new ClientTrust(clientTrustStore, brokerCertificateGenerator.getPassword()));

            var virtualCluster = new VirtualClusterBuilder(base)
                    .withClusterNetworkAddressConfigProvider(
                            new ClusterNetworkAddressConfigProviderDefinitionBuilder("SniRouting").withConfig("bootstrapAddress", virtualClusterFQDN + ":9192",
                                    "brokerAddressPattern", virtualClusterBrokerAddressPattern.formatted(i))
                                    .build())
                    .withNewTls()
                    .withNewKeyStoreKey()
                    .withStoreFile(brokerCertificateGenerator.getKeyStoreLocation())
                    .withNewInlinePasswordStore(brokerCertificateGenerator.getPassword())
                    .endKeyStoreKey()
                    .endTls()
                    .withLogNetwork(true)
                    .withLogFrames(true)
                    .build();
            builder.addToVirtualClusters("cluster" + i, virtualCluster);

        }

        try (var tester = kroxyliciousTester(builder)) {
            for (int i = 0; i < numberOfVirtualClusters; i++) {
                var trust = clientTrust.get(i);
                try (var admin = tester.admin("cluster" + i, Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trust.trustStore().toAbsolutePath().toString(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trust.password()))) {
                    // do some work to ensure virtual cluster is operational
                    createTopic(admin, TOPIC + i, 1);
                }
            }
        }
    }

    @Test
    public void exposesClusterOfTwoBrokers(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", PROXY_ADDRESS)
                                        .build())
                        .build())
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        var brokerEndpoints = Map.of(0, "localhost:" + (PROXY_ADDRESS.port() + 1), 1, "localhost:" + (PROXY_ADDRESS.port() + 2));

        try (var tester = kroxyliciousTester(builder)) {

            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString()))) {
                var nodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                var unique = nodes.stream().collect(Collectors.toMap(Node::id, ExpositionIT::toAddress));
                assertThat(unique).containsExactlyInAnyOrderEntriesOf(brokerEndpoints);
            }

            verifyAllBrokersAvailableViaProxy(tester, cluster);
        }
    }

    @Test
    public void exposedClusterAddsBroker(@BrokerCluster() KafkaCluster cluster) throws Exception {
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", PROXY_ADDRESS)
                                        .build())
                        .build())
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        try (var tester = kroxyliciousTester(builder)) {

            assertThat(cluster.getNumOfBrokers()).isEqualTo(1);
            try (var admin = tester.admin()) {
                await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                int newNodeId = cluster.addBroker();
                assertThat(cluster.getNumOfBrokers()).isEqualTo(2);

                var updatedNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                assertThat(updatedNodes).describedAs("new node should appear in the describeCluster response").anyMatch(n -> n.id() == newNodeId);
            }

            verifyAllBrokersAvailableViaProxy(tester, cluster);
        }
    }

    @Test
    public void exposedClusterRemovesBroker(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", PROXY_ADDRESS)
                                        .build())
                        .build())
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        try (var tester = kroxyliciousTester(builder)) {

            assertThat(cluster.getNumOfBrokers()).isEqualTo(2);
            try (var admin = tester.admin()) {
                await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                var removedNodeId = 1;
                cluster.removeBroker(removedNodeId);
                assertThat(cluster.getNumOfBrokers()).isEqualTo(1);

                var updatedNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                assertThat(updatedNodes).describedAs("removed node must not appear in the describeCluster response")
                        .allSatisfy(n -> assertThat(n.id()).isNotEqualTo(removedNodeId));
            }

            verifyAllBrokersAvailableViaProxy(tester, cluster);
        }
    }

    private void verifyAllBrokersAvailableViaProxy(KroxyliciousTester tester, KafkaCluster cluster) throws Exception {
        int numberOfPartitions = cluster.getNumOfBrokers();
        var topic = TOPIC + UUID.randomUUID();

        // create topic and ensure that leaders are on different brokers.
        try (var admin = tester.admin();
                var producer = tester.producer("demo", Map.of(ProducerConfig.CLIENT_ID_CONFIG, "myclient"))) {
            createTopic(admin, topic, numberOfPartitions);
            try {
                await().atMost(Duration.ofSeconds(10))
                        .ignoreExceptions()
                        .until(() -> admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get()
                                .partitions().stream().map(TopicPartitionInfo::leader)
                                .collect(Collectors.toSet()),
                                leaders -> leaders.size() == numberOfPartitions);

                for (int partition = 0; partition < numberOfPartitions; partition++) {
                    var send = producer.send(new ProducerRecord<>(topic, partition, "key", "value"));
                    send.get(10, TimeUnit.SECONDS);
                }
            }
            finally {
                deleteTopic(admin, topic);
            }
        }
    }

    private void createTopic(Admin admin, String topic, int numPartitions) {
        try {
            admin.createTopics(List.of(new NewTopic(topic, numPartitions, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTopic(Admin admin, String topic) {
        try {
            admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toAddress(Node n) {
        return n.host() + ":" + n.port();
    }
}
