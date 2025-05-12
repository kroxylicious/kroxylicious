/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.net.PassthroughProxy;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.config.VirtualClusterGatewayBuilder;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_GATEWAY_NAME;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * Integration tests that focus on the ability to present virtual clusters, with various numbers of brokers)
 * to the kafka clients
 * <br/>
 * TODO corner case test - verify kroxy's ability to recover for a temporary port already bound condition.
 */
@ExtendWith(KafkaClusterExtension.class)
class ExpositionIT extends BaseIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExpositionIT.class);

    private static final String TOPIC = "my-test-topic";
    public static final HostPort PROXY_ADDRESS = HostPort.parse("localhost:9192");

    private static final String SNI_BASE_ADDRESS = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("sni");

    public static final HostPort SNI_BOOTSTRAP = new HostPort("bootstrap." + SNI_BASE_ADDRESS, 9192);
    public static final String SNI_BROKER_ADDRESS_PATTERN = "broker-$(nodeId)." + SNI_BASE_ADDRESS;
    public static final String SASL_USER = "user";
    public static final String SASL_PASSWORD = "password";

    @TempDir
    private static Path certsDirectory;

    @ParameterizedTest
    @MethodSource("virtualClusterConfigurations")
    void exposesSingleUpstreamClusterOverTls(VirtualClusterBuilder virtualClusterBuilder,
                                             Map<String, Object> clientSecurityProtocolConfig,
                                             @BrokerCluster(numBrokers = 2) KafkaCluster cluster) {
        virtualClusterBuilder
                .withName("demo")
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster();
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(virtualClusterBuilder.build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo", clientSecurityProtocolConfig)) {
            // do some work to ensure connection is opened
            createTopic(admin, TOPIC, 1);

            var connectionsMetric = admin.metrics().entrySet().stream().filter(metricNameEntry -> "connections".equals(metricNameEntry.getKey().name()))
                    .findFirst();
            assertThat(connectionsMetric).isPresent();
            var protocol = connectionsMetric.get().getKey().tags().get("protocol");
            assertThat(protocol).startsWith("TLS");
        }
    }

    @Test
    void exposesTwoClusterOverPlainWithSeparatePorts(KafkaCluster cluster) {
        List<String> clusterProxyAddresses = List.of("localhost:9192", "localhost:9294");

        var builder = new ConfigurationBuilder();

        for (int i = 0; i < clusterProxyAddresses.size(); i++) {
            var bootstrap = HostPort.parse(clusterProxyAddresses.get(i));
            var virtualCluster = KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "cluster" + i)
                    .addToGateways(new VirtualClusterGatewayBuilder()
                            .withName(DEFAULT_GATEWAY_NAME)
                            .withNewPortIdentifiesNode()
                            .withBootstrapAddress(bootstrap)
                            .endPortIdentifiesNode()
                            .build())
                    .build();
            builder.addToVirtualClusters(virtualCluster);
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
    void exposesSingleClusterWithMultiplePortPerBrokerGateways(KafkaCluster cluster) throws Exception {
        var builder = new ConfigurationBuilder();

        VirtualClusterBuilder virtualClusterBuilder = KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "cluster");
        virtualClusterBuilder.addToGateways(portPerBrokerGateway("localhost:9192", "gateway1"),
                portPerBrokerGateway("localhost:9294", "gateway2"));
        var virtualCluster = virtualClusterBuilder.build();
        builder.addToVirtualClusters(virtualCluster);

        try (var tester = kroxyliciousTester(builder)) {
            try (var admin = tester.admin("cluster", "gateway1")) {
                createTopic(admin, TOPIC, 1);
                Set<Integer> ports = getClusterNodePorts(admin);
                assertThat(ports).containsExactly(9193);
            }
            try (var admin = tester.admin("cluster", "gateway2")) {
                createTopic(admin, TOPIC + "2", 1);
                Set<Integer> ports = getClusterNodePorts(admin);
                assertThat(ports).containsExactly(9295);
            }
        }
    }

    @Test
    void shouldFailFastWhenConnectWithSSLToPlainListener(KafkaCluster cluster) {
        assertThatThrownBy(() -> {
            try (var tester = kroxyliciousTester(proxy(cluster))) {
                String bootstrap = tester.getBootstrapAddress();
                String[] split = bootstrap.split(":");
                try (SSLSocket socket = (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket(split[0], Integer.parseInt(split[1]))) {
                    socket.setSoTimeout(5000);
                    socket.startHandshake();
                }
            }
        }).isInstanceOf(SSLHandshakeException.class).hasMessageContaining("Remote host terminated the handshake");
    }

    private static @NonNull Set<Integer> getClusterNodePorts(Admin admin) throws InterruptedException, ExecutionException, TimeoutException {
        return admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).stream().map(Node::port).collect(Collectors.toSet());
    }

    private static VirtualClusterGateway portPerBrokerGateway(String bootstrapAddress, String gatewayName) {
        return new VirtualClusterGatewayBuilder()
                .withName(gatewayName)
                .withNewPortIdentifiesNode()
                .withBootstrapAddress(HostPort.parse(bootstrapAddress))
                .endPortIdentifiesNode()
                .build();
    }

    /**
     * This is to test the case where Kroxylicious is behind yet-another-proxy that may bind to a different port than
     * Kroxylicious. For example OpenShift TLS passthrough Routes will listen on port 443 by default. The proxy container
     * won't be running as root, so binding to 443 so all the ports align isn't straightforward. Instead, we make it
     * possible for Kroxylicious to change the port it advertises its brokers at. So that clients will be told to connect
     * to the advertised port (e.g. 443) rather than the listening port for the VirtualCluster.
     */
    @Test
    void exposesUpstreamClustersUsingSniRoutingBehindPassthroughProxy(KafkaCluster cluster) throws Exception {
        try (var proxy = new PassthroughProxy(9192, "localhost")) {
            var virtualClusterCommonNamePattern = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".cluster");
            var virtualClusterBootstrapPattern = "bootstrap" + virtualClusterCommonNamePattern;
            var virtualClusterBrokerAddressPattern = "broker-$(nodeId)" + virtualClusterCommonNamePattern;

            var builder = new ConfigurationBuilder();

            var keystoreTrustStorePair = buildKeystoreTrustStorePair("*" + virtualClusterCommonNamePattern);

            var virtualCluster = KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "cluster")
                    .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder(virtualClusterBootstrapPattern + ":9192",
                            virtualClusterBrokerAddressPattern + ":" + proxy.getLocalPort())
                            .withNewTls()
                            .withNewKeyStoreKey()
                            .withStoreFile(keystoreTrustStorePair.brokerKeyStore())
                            .withNewInlinePasswordStoreProvider(keystoreTrustStorePair.password())
                            .endKeyStoreKey()
                            .endTls()
                            .build())
                    .withLogNetwork(true)
                    .withLogFrames(true)
                    .build();
            builder.addToVirtualClusters(virtualCluster);

            try (var tester = kroxyliciousTester(builder)) {
                // the tester is aware that it should connect to the Virtual Cluster's advertised port
                try (var admin = tester.admin("cluster", Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keystoreTrustStorePair.clientTrustStore(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, keystoreTrustStorePair.password()))) {
                    admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).forEach(node -> {
                        assertThat(node.port()).isEqualTo(proxy.getLocalPort());
                    });
                    // do some work to ensure virtual cluster is operational
                    createTopic(admin, TOPIC, 1);
                }
            }
        }
    }

    @Test
    void exposesTwoSeparateUpstreamClustersUsingSniRouting(KafkaCluster cluster) throws Exception {
        var keystoreTrustStoreList = new ArrayList<KeystoreTrustStorePair>();
        var virtualClusterCommonNamePattern = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".virtualcluster%d");
        var virtualClusterBootstrapPattern = "bootstrap" + virtualClusterCommonNamePattern;
        var virtualClusterBrokerAddressPattern = "broker-$(nodeId)" + virtualClusterCommonNamePattern;

        var builder = new ConfigurationBuilder();

        int numberOfVirtualClusters = 2;
        for (int i = 0; i < numberOfVirtualClusters; i++) {
            var virtualClusterFQDN = virtualClusterBootstrapPattern.formatted(i);
            var keystoreTrustStorePair = buildKeystoreTrustStorePair("*" + virtualClusterCommonNamePattern.formatted(i));
            keystoreTrustStoreList.add(keystoreTrustStorePair);

            var virtualCluster = KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "cluster" + i)
                    .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder(virtualClusterFQDN + ":9192", virtualClusterBrokerAddressPattern.formatted(i))
                            .withNewTls()
                            .withNewKeyStoreKey()
                            .withStoreFile(keystoreTrustStorePair.brokerKeyStore())
                            .withNewInlinePasswordStoreProvider(keystoreTrustStorePair.password())
                            .endKeyStoreKey()
                            .endTls()
                            .build())
                    .withLogNetwork(true)
                    .withLogFrames(true)
                    .build();
            builder.addToVirtualClusters(virtualCluster);
        }

        try (var tester = kroxyliciousTester(builder)) {
            for (int i = 0; i < numberOfVirtualClusters; i++) {
                var trust = keystoreTrustStoreList.get(i);
                try (var admin = tester.admin("cluster" + i, Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trust.clientTrustStore(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trust.password()))) {
                    // do some work to ensure virtual cluster is operational
                    createTopic(admin, TOPIC + i, 1);
                }
            }
        }
    }

    @Test
    void exposesSingleUpstreamClustersUsingMultipleSniGateways(KafkaCluster cluster) throws Exception {
        var keystoreTrustStoreList = new ArrayList<KeystoreTrustStorePair>();
        var virtualClusterCommonNamePattern = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".virtualcluster%d");
        var virtualClusterBootstrapPattern = "bootstrap" + virtualClusterCommonNamePattern;
        var virtualClusterBrokerAddressPattern = "broker-$(nodeId)" + virtualClusterCommonNamePattern;

        var builder = new ConfigurationBuilder();

        int numberOfGateways = 2;
        VirtualClusterBuilder virtualClusterBuilder = KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "cluster");
        for (int i = 0; i < numberOfGateways; i++) {
            var virtualClusterFQDN = virtualClusterBootstrapPattern.formatted(i);
            var keystoreTrustStorePair = buildKeystoreTrustStorePair("*" + virtualClusterCommonNamePattern.formatted(i));
            keystoreTrustStoreList.add(keystoreTrustStorePair);
            virtualClusterBuilder
                    .addToGateways(new VirtualClusterGatewayBuilder()
                            .withName("gateway-" + i)
                            .withNewSniHostIdentifiesNode()
                            .withBootstrapAddress(new HostPort(virtualClusterFQDN, 9192))
                            .withAdvertisedBrokerAddressPattern(virtualClusterBrokerAddressPattern.formatted(i))
                            .endSniHostIdentifiesNode()
                            .withNewTls()
                            .withNewKeyStoreKey()
                            .withStoreFile(keystoreTrustStorePair.brokerKeyStore())
                            .withNewInlinePasswordStoreProvider(keystoreTrustStorePair.password())
                            .endKeyStoreKey()
                            .endTls()
                            .build())
                    .withLogNetwork(true)
                    .withLogFrames(true)
                    .build();
        }
        builder.addToVirtualClusters(virtualClusterBuilder.build());

        try (var tester = kroxyliciousTester(builder)) {
            for (int i = 0; i < numberOfGateways; i++) {
                var trust = keystoreTrustStoreList.get(i);
                try (var admin = tester.admin("cluster", "gateway-" + i, Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trust.clientTrustStore(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trust.password()))) {
                    // do some work to ensure virtual cluster is operational
                    createTopic(admin, TOPIC + i, 1);
                    Set<String> hosts = admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).stream().map(Node::host).collect(Collectors.toSet());
                    assertThat(hosts).containsExactly(virtualClusterBrokerAddressPattern.formatted(i).replace("$(nodeId)", "0"));
                }
            }
        }
    }

    @Test
    void exposesClusterOfTwoBrokersWithRangeAwarePortPerNode(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultGatewayBuilder()
                                .withNewPortIdentifiesNode()
                                .withBootstrapAddress(PROXY_ADDRESS)
                                .withNodeIdRanges(new NamedRange("nodes", 0, 2))
                                .endPortIdentifiesNode()
                                .build())
                        .build());

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
    void exposesClusterOfTwoBrokersWithGapInNodeIds(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        cluster.addBroker();
        cluster.removeBroker(1);
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultGatewayBuilder()
                                .withNewPortIdentifiesNode()
                                .withBootstrapAddress(PROXY_ADDRESS)
                                .withNodeIdRanges(new NamedRange("node-0", 0, 0), new NamedRange("node-2", 2, 2))
                                .endPortIdentifiesNode()
                                .build())
                        .build());

        var brokerEndpoints = Map.of(0, "localhost:" + (PROXY_ADDRESS.port() + 1), 2, "localhost:" + (PROXY_ADDRESS.port() + 2));

        try (var tester = kroxyliciousTester(builder)) {

            try (var admin = CloseableAdmin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PROXY_ADDRESS.toString()))) {
                var nodes = await().atMost(Duration.ofSeconds(20)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                var unique = nodes.stream().collect(Collectors.toMap(Node::id, ExpositionIT::toAddress));
                assertThat(unique).containsExactlyInAnyOrderEntriesOf(brokerEndpoints);
            }

            verifyAllBrokersAvailableViaProxy(tester, cluster);
        }
    }

    @Test
    void exposesClusterOfTwoBrokers(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        HostPort proxyAddress = PROXY_ADDRESS;
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(proxyAddress).build())
                        .build());

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

    private static Stream<Arguments> virtualClusterConfigurations() throws Exception {
        var portPerBrokerKeystoreTrustStorePair = buildKeystoreTrustStorePair("localhost");
        var sniKeystoreTrustStorePair = buildKeystoreTrustStorePair("*." + SNI_BASE_ADDRESS);

        return Stream.of(
                argumentSet("PortIdentifiesNode",
                        new VirtualClusterBuilder()
                                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                        .withNewTls()
                                        .withNewKeyStoreKey()
                                        .withStoreFile(portPerBrokerKeystoreTrustStorePair.brokerKeyStore())
                                        .withNewInlinePasswordStoreProvider(portPerBrokerKeystoreTrustStorePair.password())
                                        .endKeyStoreKey()
                                        .endTls()
                                        .build()),
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, portPerBrokerKeystoreTrustStorePair.clientTrustStore(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, portPerBrokerKeystoreTrustStorePair.password())),
                argumentSet("SniHostIdentifiesNode",
                        new VirtualClusterBuilder()
                                .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder(SNI_BOOTSTRAP.toString(), SNI_BROKER_ADDRESS_PATTERN)
                                        .withNewTls()
                                        .withNewKeyStoreKey()
                                        .withStoreFile(sniKeystoreTrustStorePair.brokerKeyStore())
                                        .withNewInlinePasswordStoreProvider(sniKeystoreTrustStorePair.password())
                                        .endKeyStoreKey()
                                        .endTls()
                                        .build()),
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sniKeystoreTrustStorePair.clientTrustStore(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sniKeystoreTrustStorePair.password())));
    }

    /**
     * @see #connectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart(VirtualClusterBuilder, Map, KafkaCluster)
     * @param virtualClusterBuilder the virtual cluster builder
     * @param clientSecurityProtocolConfig addition client configuration
     * @param cluster kafka cluster
     */
    @ParameterizedTest()
    @MethodSource(value = "virtualClusterConfigurations")
    void connectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart(VirtualClusterBuilder virtualClusterBuilder,
                                                                         Map<String, Object> clientSecurityProtocolConfig,
                                                                         @BrokerCluster(numBrokers = 2) KafkaCluster cluster) {
        doConnectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart(virtualClusterBuilder, clientSecurityProtocolConfig, cluster);
    }

    /**
     * @see #connectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart(VirtualClusterBuilder, Map, KafkaCluster)
     * @param virtualClusterBuilder the virtual cluster builder
     * @param clientSecurityProtocolConfig addition client configuration
     * @param cluster kafka cluster
     */
    @ParameterizedTest()
    @MethodSource(value = "virtualClusterConfigurations")
    void connectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart_Sasl(VirtualClusterBuilder virtualClusterBuilder,
                                                                              Map<String, Object> clientSecurityProtocolConfig,
                                                                              @BrokerCluster(numBrokers = 2) @SaslMechanism(principals = {
                                                                                      @SaslMechanism.Principal(user = SASL_USER, password = SASL_PASSWORD) }) KafkaCluster cluster) {

        final Optional<Tls> tls = virtualClusterBuilder.buildFirstGateway().tls();
        SecurityProtocol securityProtocol = tls.isPresent() ? SecurityProtocol.SASL_SSL : SecurityProtocol.SASL_PLAINTEXT;
        clientSecurityProtocolConfig = new HashMap<>(clientSecurityProtocolConfig);
        clientSecurityProtocolConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
        clientSecurityProtocolConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format("""
                        %s required username="%s" password="%s";""",
                        PlainLoginModule.class.getName(), SASL_USER, SASL_PASSWORD));
        clientSecurityProtocolConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        doConnectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart(virtualClusterBuilder, clientSecurityProtocolConfig, cluster);
    }

    /**
     * This test ensures that Kroxylicious, on startup, exposes all the brokers of the target cluster
     * without requiring that a client connects to bootstrap first.  This is important for resilience: even though
     * Kafka clients are configured with a bootstrap address there is no guarantee that the client will reconsult
     * bootstrap when it is trying to re-establish lost connections to brokers it already knows about.
     *
     * @param virtualClusterBuilder the virtual cluster builder
     * @param clientSecurityProtocolConfig addition client configuration
     * @param cluster kafka cluster
     */
    private void doConnectToExposedBrokerEndpointsDirectlyAfterKroxyliciousRestart(VirtualClusterBuilder virtualClusterBuilder,
                                                                                   Map<String, Object> clientSecurityProtocolConfig,
                                                                                   KafkaCluster cluster) {
        virtualClusterBuilder
                .withName("demo")
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(virtualClusterBuilder.build());

        // First, learn the broker endpoints.

        Collection<Node> originalNodes;
        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin(clientSecurityProtocolConfig)) {
            originalNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                    n -> n.size() == cluster.getNumOfBrokers());
        }

        LOGGER.debug("Bootstrap discovered broker nodes {}", originalNodes);

        // Now, iterate across the learnt broker endpoints, connecting the client to each in turn verifying that the
        // client discovers the remainder of the cluster. Note the Kroxylicious restart on each iteration, this
        // ensures it has zero-state.
        originalNodes.forEach(node -> {
            LOGGER.debug("Testing connection to {}", node);
            var brokerAddress = toAddress(node);
            var brokerConfig = new HashMap<String, Object>(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerAddress));
            brokerConfig.putAll(clientSecurityProtocolConfig);

            try (var tester = kroxyliciousTester(builder);
                    var admin = tester.admin(brokerConfig)) {
                var rediscoveredNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                assertThat(rediscoveredNodes).containsExactlyElementsOf(originalNodes);
            }
        });
    }

    @ParameterizedTest
    @MethodSource(value = "virtualClusterConfigurations")
    void connectToDiscoveryAddress(VirtualClusterBuilder virtualClusterBuilder,
                                   Map<String, Object> clientSecurityProtocolConfig,
                                   @BrokerCluster KafkaCluster cluster) {
        virtualClusterBuilder
                .withName("demo")
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster();
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(virtualClusterBuilder.build());

        final HostPort discoveryBrokerAddressToProbe;
        if (virtualClusterBuilder.buildFirstGateway().sniHostIdentifiesNode() != null) {
            discoveryBrokerAddressToProbe = new HostPort(SNI_BROKER_ADDRESS_PATTERN.replace("$(nodeId)", Integer.toString(cluster.getNumOfBrokers())),
                    SNI_BOOTSTRAP.port());
        }
        else {
            discoveryBrokerAddressToProbe = new HostPort(PROXY_ADDRESS.host(), PROXY_ADDRESS.port() + cluster.getNumOfBrokers() + 1);
        }

        // precondition: verify that discoveryBrokerAddressToProbe isn't actually a broker address
        Collection<Node> originalNodes;
        try (var tester = kroxyliciousTester(builder)) {
            try (var admin = tester.admin(clientSecurityProtocolConfig)) {
                originalNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                var hostPorts = originalNodes.stream().map(n -> new HostPort(n.host(), n.port())).collect(Collectors.toSet());
                assertThat(hostPorts)
                        .describedAs("test precondition broken - fail to properly deduce a pre-bound broker address. deduced %s, cluster's nodes %s",
                                discoveryBrokerAddressToProbe,
                                originalNodes)
                        .isNotEmpty()
                        .doesNotContain(discoveryBrokerAddressToProbe);
            }
        }

        try (var tester = kroxyliciousTester(builder)) {
            var brokerConfig = new HashMap<String, Object>(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, discoveryBrokerAddressToProbe.toString()));
            brokerConfig.putAll(clientSecurityProtocolConfig);

            try (var admin = tester.admin(brokerConfig)) {
                var rediscoveredNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                assertThat(rediscoveredNodes).containsExactlyElementsOf(originalNodes);
            }

            // Now dial in a second time on the same pre-bound address, this confirms that kroxylicious continues to listen, even though
            // it has reconciled.
            try (var admin = tester.admin(brokerConfig)) {
                var rediscoveredNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());
                assertThat(rediscoveredNodes).containsExactlyElementsOf(originalNodes);
            }
        }
    }

    @Test
    void targetClusterDynamicallyAddsBroker(@BrokerCluster KafkaCluster cluster) throws Exception {
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder)) {

            assertThat(cluster.getNumOfBrokers()).isOne();
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

    // Test extension does not allow us to influence the node ids, so we start a 3 node cluster
    // and shutdown node 1, leaving us with nodes 0 (controller/broker) and 2 (broker).
    @Test
    void canConfigurePortIdentifiesNodeWithRanges(@BrokerCluster(numBrokers = 3) KafkaCluster cluster, Admin admin) throws Exception {
        cluster.removeBroker(1);
        await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                n -> n.stream().map(Node::id).collect(Collectors.toSet()).equals(Set.of(0, 2)));
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultGatewayBuilder()
                                .withNewPortIdentifiesNode()
                                .withBootstrapAddress(PROXY_ADDRESS)
                                .withNodeIdRanges(new NamedRange("range1", 0, 0), new NamedRange("range2", 2, 2))
                                .endPortIdentifiesNode()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder)) {
            assertThat(cluster.getNumOfBrokers()).isEqualTo(2);
            verifyAllBrokersAvailableViaProxy(tester, cluster);
        }
    }

    @Test
    void targetClusterDynamicallyRemovesBroker(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder)) {

            assertThat(cluster.getNumOfBrokers()).isEqualTo(2);
            try (var admin = tester.admin()) {
                await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                var removedNodeId = 1;
                cluster.removeBroker(removedNodeId);
                assertThat(cluster.getNumOfBrokers()).isOne();

                var updatedNodes = await().atMost(Duration.ofSeconds(5)).until(() -> admin.describeCluster().nodes().get(),
                        n -> n.size() == cluster.getNumOfBrokers());

                assertThat(updatedNodes).describedAs("removed node must not appear in the describeCluster response")
                        .isNotEmpty()
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
                deleteTopics(admin, TopicNameCollection.ofTopicNames(List.of(topic)));
            }
        }
    }

    private static String toAddress(Node n) {
        return n.host() + ":" + n.port();
    }

    private record KeystoreTrustStorePair(String brokerKeyStore, String clientTrustStore, String password) {}

    @NonNull
    private static ExpositionIT.KeystoreTrustStorePair buildKeystoreTrustStorePair(String domain) throws Exception {
        var brokerCertificateGenerator = new KeytoolCertificateGenerator();
        brokerCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", domain, "KI", "kroxylicious.io", null, null, "US");
        Path resolve = certsDirectory.resolve(UUID.randomUUID().toString());
        var unused = resolve.toFile().mkdirs();
        var clientTrustStore = resolve.resolve("kafka.truststore.jks");
        brokerCertificateGenerator.generateTrustStore(brokerCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());
        return new KeystoreTrustStorePair(brokerCertificateGenerator.getKeyStoreLocation(), clientTrustStore.toAbsolutePath().toString(),
                brokerCertificateGenerator.getPassword());
    }
}
