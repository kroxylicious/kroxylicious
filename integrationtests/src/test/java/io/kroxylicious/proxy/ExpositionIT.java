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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.proxy.Utils.startProxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Integration tests that focus on how Kroxylicious presents itself to the network.
 */
@ExtendWith(KafkaClusterExtension.class)
public class ExpositionIT {

    private static final String TOPIC = "my-test-topic";

    @TempDir
    private Path certsDirectory;

    private record ClientTrust(Path trustStore, String password) {
    };

    @Test
    public void exposesClusterOverTls(KafkaCluster cluster) throws Exception {
        String proxyAddress = "localhost:9192";

        var brokerCertificateGenerator = new KeytoolCertificateGenerator();
        brokerCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "localhost", "KI", "RedHat", null, null, "US");
        var clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        brokerCertificateGenerator.generateTrustStore(brokerCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        var builder = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers());
        var demo = builder.getVirtualClusters().get("demo");
        demo = new VirtualClusterBuilder(demo)
                .withKeyPassword(brokerCertificateGenerator.getPassword())
                .withKeyStoreFile(brokerCertificateGenerator.getKeyStoreLocation())
                .build();
        builder.addToVirtualClusters("demo", demo);
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {
            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
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
    public void exposesTwoClusterOverPlainWithSeparatePorts(KafkaCluster cluster) throws Exception {
        List<String> clusterProxyAddresses = List.of("localhost:9192", "localhost:9193");

        var builder = KroxyConfig.builder()
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();

        var base = new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .build();

        for (int i = 0; i < clusterProxyAddresses.size(); i++) {
            var virtualCluster = new VirtualClusterBuilder(base)
                    .withNewClusterEndpointConfigProvider()
                    .withType("StaticCluster")
                    .withConfig(Map.of("bootstrapAddress", clusterProxyAddresses.get(i)))
                    .endClusterEndpointConfigProvider()
                    .build();
            builder.addToVirtualClusters("cluster" + i, virtualCluster);

        }
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {
            for (int i = 0; i < clusterProxyAddresses.size(); i++) {
                try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterProxyAddresses.get(i)))) {
                    // do some work to ensure virtual cluster is operational
                    createTopic(admin, TOPIC + i, 1);
                }
            }
        }
    }

    @Test
    public void exposesTwoClusterOverTlsWithSharedPort(KafkaCluster cluster) throws Exception {
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
                    .withType("SniAware")
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
        var proxyAddress = "localhost:9192";
        var builder = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers());
        var demo = builder.getVirtualClusters().get("demo");
        var brokerEndpoints = Map.of(0, "localhost:9193", 1, "localhost:9194");
        demo = new VirtualClusterBuilder(demo)
                .editClusterEndpointConfigProvider()
                .withType("StaticCluster")
                .withConfig(Map.of("bootstrapAddress", proxyAddress,
                        "brokers", brokerEndpoints))
                .endClusterEndpointConfigProvider()
                .build();
        builder.addToVirtualClusters("demo", demo);
        var config = builder.build().toYaml();

        try (var proxy = startProxy(config)) {

            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress))) {
                var nodes = admin.describeCluster().nodes().get();
                assertThat(nodes).hasSize(2);
                var unique = nodes.stream().collect(Collectors.toMap(Node::id, ExpositionIT::toAddress));
                assertThat(unique).containsExactlyInAnyOrderEntriesOf(brokerEndpoints);
            }

            // Create a topic with one partition on each broker and publish to all of them..
            // As kafka client must connect to the partition leader, this verifies that kroxylicious is
            // indeed connecting to all brokers.
            int numPartitions = 2;
            try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress))) {
                // create topic and ensure that leaders are on different brokers.
                createTopic(admin, TOPIC, numPartitions);
                await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeTopics(List.of(TOPIC)).topicNameValues().get(TOPIC).get()
                        .partitions().stream().map(TopicPartitionInfo::leader)
                        .collect(Collectors.toSet()),
                        leaders -> leaders.size() == numPartitions);

                try (var producer = new KafkaProducer<String, String>(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                        ProducerConfig.CLIENT_ID_CONFIG, "myclient",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
                    for (int partition = 0; partition < numPartitions; partition++) {
                        var send = producer.send(new ProducerRecord<>(TOPIC, partition, "key", "value"));
                        send.get(10, TimeUnit.SECONDS);
                    }
                }
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

    private static String toAddress(Node n) {
        return n.host() + ":" + n.port();
    }

    private static KroxyConfigBuilder baseConfigBuilder(String proxyAddress, String bootstrapServers) {
        return KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("StaticCluster")
                        .withConfig(Map.of("bootstrapAddress", proxyAddress))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter();
    }

}
