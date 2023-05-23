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
import java.util.stream.Stream;

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
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.proxy.Utils.startProxy;
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
    };

    @Test
    public void exposesSingleUpstreamClusterOverTls(KafkaCluster cluster) throws Exception {

        var brokerCertificateGenerator = new KeytoolCertificateGenerator();
        brokerCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "localhost", "KI", "RedHat", null, null, "US");
        var clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        brokerCertificateGenerator.generateTrustStore(brokerCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        String bootstrapServers = cluster.getBootstrapServers();

        var builder = KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("PortPerBroker")
                        .withConfig(Map.of("bootstrapAddress", PROXY_ADDRESS.toString()))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();

        var demo = builder.getVirtualClusters().get("demo");
        demo = new VirtualClusterBuilder(demo)
                .withKeyPassword(brokerCertificateGenerator.getPassword())
                .withKeyStoreFile(brokerCertificateGenerator.getKeyStoreLocation())
                .build();
        builder.addToVirtualClusters("demo", demo);
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {
            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString(),
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
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
    }

    @Test
    public void exposesTwoSeparateUpstreamClusters(KafkaCluster cluster) throws Exception {
        var clusterProxyAddresses = Stream.of("localhost:9192", "localhost:9294").map(HostPort::parse).toList();

        var builder = KroxyConfig.builder()
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();

        var base = new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .build();

        for (int i = 0; i < clusterProxyAddresses.size(); i++) {
            var bootstrap = clusterProxyAddresses.get(i);
            var virtualCluster = new VirtualClusterBuilder(base)
                    .withNewClusterEndpointConfigProvider()
                    .withType("PortPerBroker")
                    .withConfig(Map.of("bootstrapAddress", bootstrap.toString()))
                    .endClusterEndpointConfigProvider()
                    .build();
            builder.addToVirtualClusters("cluster" + i, virtualCluster);

        }
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {
            for (int i = 0; i < clusterProxyAddresses.size(); i++) {
                try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterProxyAddresses.get(i).toString()))) {
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

        var builder = KroxyConfig.builder()
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();

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
                    .withNewClusterEndpointConfigProvider()
                    .withType("SniRouting")
                    .withConfig(Map.of("bootstrapAddress", virtualClusterFQDN + ":9192",
                            "brokerAddressPattern", virtualClusterBrokerAddressPattern.formatted(i)))
                    .endClusterEndpointConfigProvider()
                    .withKeyPassword(brokerCertificateGenerator.getPassword())
                    .withKeyStoreFile(brokerCertificateGenerator.getKeyStoreLocation())
                    .withLogNetwork(true)
                    .withLogFrames(true)
                    .build();
            builder.addToVirtualClusters("cluster" + i, virtualCluster);

        }
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {
            for (int i = 0; i < numberOfVirtualClusters; i++) {
                var trust = clientTrust.get(i);
                try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, virtualClusterBootstrapPattern.formatted(i) + ":9192",
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

        var builder = KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("PortPerBroker")
                        .withConfig(Map.of("bootstrapAddress", PROXY_ADDRESS.toString()))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();
        var config = builder.build().toYaml();

        var brokerEndpoints = Map.of(0, "localhost:9193", 1, "localhost:9194");

        try (var proxy = startProxy(config)) {

            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString()))) {
                var nodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                var unique = nodes.stream().collect(Collectors.toMap(Node::id, ExpositionIT::toAddress));
                assertThat(unique).containsExactlyInAnyOrderEntriesOf(brokerEndpoints);
            }

            try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString()))) {
                // create topic and ensure that leaders are on different brokers.
                verifyAllBrokersAvailableViaProxy(PROXY_ADDRESS, cluster);
            }
        }
    }

    @Test
    public void exposedClusterAddsBroker(@BrokerCluster() KafkaCluster cluster) throws Exception {
        var builder = KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("PortPerBroker")
                        .withConfig(Map.of("bootstrapAddress", PROXY_ADDRESS.toString()))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {

            assertThat(cluster.getNumOfBrokers()).isEqualTo(1);
            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString()))) {
                await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                int newNodeId = cluster.addBroker();
                assertThat(cluster.getNumOfBrokers()).isEqualTo(2);

                var updatedNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                assertThat(updatedNodes).describedAs("new node should appear in the describeCluster response").anyMatch(n -> n.id() == newNodeId);
            }

            verifyAllBrokersAvailableViaProxy(PROXY_ADDRESS, cluster);
        }
    }

    @Test
    public void exposedClusterRemovesBroker(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        var builder = KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("PortPerBroker")
                        .withConfig(Map.of("bootstrapAddress", PROXY_ADDRESS.toString()))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {

            assertThat(cluster.getNumOfBrokers()).isEqualTo(2);
            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString()))) {
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

            verifyAllBrokersAvailableViaProxy(PROXY_ADDRESS, cluster);
        }
    }

    private void verifyAllBrokersAvailableViaProxy(HostPort proxyAddress, KafkaCluster cluster) throws Exception {
        int numberOfPartitions = cluster.getNumOfBrokers();
        var topic = TOPIC + UUID.randomUUID();

        // create topic and ensure that leaders are on different brokers.
        try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString()))) {
            createTopic(admin, topic, numberOfPartitions);
            try {
                await().atMost(Duration.ofSeconds(10))
                        .ignoreExceptions()
                        .until(() -> admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get()
                                .partitions().stream().map(TopicPartitionInfo::leader)
                                .collect(Collectors.toSet()),
                                leaders -> leaders.size() == numberOfPartitions);

                // now producer to all partitions. this proves that all proxies are available.
                try (var producer = CloseableProducer.<String, String> create(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
                        ProducerConfig.CLIENT_ID_CONFIG, "myclient",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
                    for (int partition = 0; partition < numberOfPartitions; partition++) {
                        var send = producer.send(new ProducerRecord<>(topic, partition, "key", "value"));
                        send.get(10, TimeUnit.SECONDS);
                    }
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
