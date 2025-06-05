/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.secret.FilePassword;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.Request;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaClusterFactory;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.common.Tls;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.baseVirtualClusterBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests focused on Kroxylicious ability to use TLS for both the upstream and downstream.
 * <p>
 * TODO add integration tests covering kroylicious's ability to use JKS and PEM material. Needs <a href="https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/120">issues#120</a>
 */
@ExtendWith(KafkaClusterExtension.class)
class TlsIT extends BaseIT {
    private static final HostPort PROXY_ADDRESS = HostPort.parse("localhost:9192");
    private static final String TOPIC = "my-test-topic";

    private static final String SNI_BASE_ADDRESS = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("sni");
    private static final String SNI_BROKER_ADDRESS_PATTERN = "broker-$(nodeId)." + SNI_BASE_ADDRESS;
    private static final HostPort SNI_BOOTSTRAP_ADDRESS = HostPort.parse("bootstrap." + SNI_BASE_ADDRESS + ":9192");

    @TempDir
    private Path certsDirectory;
    private KeytoolCertificateGenerator downstreamCertificateGenerator;
    private KeytoolCertificateGenerator clientCertGenerator;
    private Path clientTrustStore;
    private Path proxyTrustStore;

    @BeforeEach
    void beforeEach() throws Exception {
        // Note that the KeytoolCertificateGenerator generates key stores that are PKCS12 format.
        this.downstreamCertificateGenerator = new KeytoolCertificateGenerator();
        this.downstreamCertificateGenerator.generateSelfSignedCertificateEntry("test@kroxylicious.io", "localhost", "KI", "kroxylicious.io", null, null, "US");
        this.clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        this.downstreamCertificateGenerator.generateTrustStore(this.downstreamCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        // Generator for certificate that will identify the client
        this.clientCertGenerator = new KeytoolCertificateGenerator();
        this.clientCertGenerator.generateSelfSignedCertificateEntry("clientTest@kroxylicious.io", "client", "Dev", "kroxylicious.io", null, null, "US");
        this.proxyTrustStore = certsDirectory.resolve("proxy.truststore.jks");
        this.clientCertGenerator.generateTrustStore(clientCertGenerator.getCertFilePath(), "proxy",
                proxyTrustStore.toAbsolutePath().toString());
    }

    @Test
    void upstreamUsesSelfSignedTls_TrustStore(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withNewInlinePasswordStoreProvider(brokerTruststorePassword)
                        .endTrustStoreTrust()
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder); var admin = tester.admin("demo")) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstreamConnectionValidatesHostnames(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers.replace("localhost", "127.0.0.1"))
                        // 127.0.0.1 is not included as Subject Alternate Name (SAN) so hostname validation will fail.
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withNewInlinePasswordStoreProvider(brokerTruststorePassword)
                        .endTrustStoreTrust()
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder); var admin = tester.admin("demo")) {
            // do some work to ensure connection is opened
            assertThat(admin.describeCluster(new DescribeClusterOptions().timeoutMs(10_000)).clusterId())
                    .failsWithin(Duration.ofSeconds(30))
                    .withThrowableThat()
                    .withCauseInstanceOf(TimeoutException.class)
                    .havingCause()
                    .withMessageStartingWith("Timed out waiting for a node assignment.");
        }
    }

    @Test
    void upstreamUsesSelfSignedTls_TrustX509(@Tls KafkaCluster cluster) throws Exception {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var trustStore = KeyStore.getInstance(new File(brokerTruststore), brokerTruststorePassword.toCharArray());
        var params = new PKIXParameters(trustStore);

        var trustAnchors = params.getTrustAnchors();
        var certificates = trustAnchors.stream().map(TrustAnchor::getTrustedCert).toList();
        assertThat(certificates).isNotNull()
                .hasSizeGreaterThan(0);

        var file = writeTrustToTemporaryFile(certificates);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(file.getAbsolutePath())
                        .withStoreType("PEM")
                        .endTrustStoreTrust()
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder); var admin = tester.admin("demo")) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstreamUsesTlsInsecure(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewInsecureTlsTrust(true)
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                        .build());

        try (var tester = kroxyliciousTester(builder); var admin = tester.admin("demo")) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstreamRequiresTlsClientAuth_TrustStore() throws Exception {
        // Note that the annotation driven Kroxylicious Extension doesn't support configuring a cluster that expects client-auth.

        var brokerCert = new KeytoolCertificateGenerator();
        var clientCert = new KeytoolCertificateGenerator();
        clientCert.generateSelfSignedCertificateEntry("clientTest@kroxylicious.io", "client", "Dev", "Kroxylicious.io", null, null, "US");

        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .brokerKeytoolCertificateGenerator(brokerCert)
                // note passing client generator causes ssl.client.auth to be set 'required'
                .clientKeytoolCertificateGenerator(clientCert)
                .kraftMode(true)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();

            var config = new HashMap<>(cluster.getKafkaClientConfiguration());

            // Get the client key material provided by the framework. This will be used to configure Kroxylicious.
            var keyStore = config.get("ssl.keystore.location").toString();
            var keyPassword = config.get("ssl.keystore.password").toString();
            assertThat(keyStore).isNotNull();
            assertThat(keyPassword).isNotNull();

            var trustStore = config.get("ssl.truststore.location").toString();
            var trustPassword = config.get("ssl.truststore.password").toString();
            assertThat(trustStore).isNotNull();
            assertThat(trustPassword).isNotNull();

            // Validate a TLS client-auth connection to direct the cluster succeeds/fails as expected.
            assertSuccessfulDirectClientAuthConnectionWithClientCert(cluster);
            assertUnsuccessfulDirectClientAuthConnectionWithoutClientCert(cluster);

            var builder = new ConfigurationBuilder()
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("demo")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .withNewTls()
                            .withNewTrustStoreTrust()
                            .withStoreFile(trustStore)
                            .withNewInlinePasswordStoreProvider(trustPassword)
                            .endTrustStoreTrust()
                            .withNewKeyStoreKey()
                            .withStoreFile(keyStore)
                            .withNewInlinePasswordStoreProvider(keyPassword)
                            .endKeyStoreKey()
                            .endTls()
                            .endTargetCluster()
                            .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                            .build());

            try (var tester = kroxyliciousTester(builder); var admin = tester.admin("demo")) {
                // do some work to ensure connection is opened
                final var result = admin.describeCluster().clusterId();
                assertThat(result).as("Unable to get the clusterId from the Kafka cluster").succeedsWithin(Duration.ofSeconds(10));
            }
        }
    }

    private void assertSuccessfulDirectClientAuthConnectionWithClientCert(KafkaCluster cluster) {
        try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
            // Any operation to test successful connection to cluster
            var result = admin.describeCluster().clusterId();
            assertThat(result).succeedsWithin(Duration.ofSeconds(10));
        }
    }

    private void assertUnsuccessfulDirectClientAuthConnectionWithoutClientCert(KafkaCluster cluster) {
        var config = new HashMap<>(cluster.getKafkaClientConfiguration());

        // remove the client's certificate that the framework has provides.
        assertThat(config.remove("ssl.keystore.location")).isNotNull();
        assertThat(config.remove("ssl.keystore.password")).isNotNull();

        try (var admin = CloseableAdmin.create(config)) {
            // Any operation to test that connection to cluster fails as we don't present a certificate.
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(SSLHandshakeException.class)
                    .hasRootCauseMessage("Received fatal alert: bad_certificate");
        }
    }

    @ParameterizedTest
    @ValueSource(classes = { InlinePassword.class, FilePassword.class })
    void downstreamAndUpstreamTls(Class<? extends PasswordProvider> providerClazz, @Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();
        var proxyKeystoreLocation = downstreamCertificateGenerator.getKeyStoreLocation();
        var proxyKeystorePassword = downstreamCertificateGenerator.getPassword();

        var brokerTrustPasswordProvider = constructPasswordProvider(providerClazz, brokerTruststorePassword);
        var proxyKeystorePasswordProvider = constructPasswordProvider(providerClazz, proxyKeystorePassword);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withStorePasswordProvider(brokerTrustPasswordProvider)
                        .endTrustStoreTrust()
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(proxyKeystoreLocation)
                                .withStorePasswordProvider(proxyKeystorePasswordProvider)
                                .endKeyStoreKey()
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, proxyKeystorePassword))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void downstream_SuccessfulTlsWithProtocolsAllowed(KafkaCluster cluster) {
        // Protocol we want to use
        AllowDeny<String> protocols = new AllowDeny<>(List.of("TLSv1.2"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withProtocols(protocols)
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword(),
                                // Accepted Protocol matches what we want to use
                                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2"))) {

            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();

            // verify that the admin is actually using the intended protocol
            assertThat(admin)
                    .extracting("instance")
                    .extracting("client")
                    .extracting("selector")
                    .extracting("channels", InstanceOfAssertFactories.map(String.class, Object.class))
                    .anySatisfy((id, channel) -> {
                        assertThat(channel)
                                .extracting("transportLayer")
                                .extracting("sslEngine", InstanceOfAssertFactories.type(SSLEngine.class))
                                .extracting(SSLEngine::getSession)
                                .extracting(SSLSession::getProtocol)
                                .isEqualTo("TLSv1.2");
                    });

        }
    }

    @Test
    void downstream_UnrecognizedSniHostNameClosesConnection(KafkaCluster cluster) {
        var duffBootstrap = "bootstrap." + IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("duff") + ":" + SNI_BOOTSTRAP_ADDRESS.port();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder(SNI_BOOTSTRAP_ADDRESS, SNI_BROKER_ADDRESS_PATTERN)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var testClient = tester.simpleTestClient(duffBootstrap, true)) {

            // The request itself is unimportant, it won't actually reach the server
            var request = new Request(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(), null, new ApiVersionsRequestData());
            var response = testClient.get(request);
            assertThat(response)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withMessageContaining("channel closed before response received!");
        }
    }

    @Test
    void downstream_UntrustedCertificateClosesConnection(KafkaCluster cluster) {

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                // admin won't trust the self signed cert of the broker.
                var admin = tester.admin(Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name))) {

            assertThat(admin.describeCluster().clusterId())
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableThat()
                    .withCauseInstanceOf(SslAuthenticationException.class)
                    .withMessageContaining("SSL handshake failed");
        }
    }

    @Test
    void downstream_UnsuccessfulTlsWithProtocolsAllowed(KafkaCluster cluster) {
        // Protocol we want to use
        AllowDeny<String> protocols = new AllowDeny<>(List.of("TLSv1.2"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withProtocols(protocols)
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword(),
                                // Accepted Protocol doesn't match what we want to use
                                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3"))) {
            // Server will only allow us to use TLSv1.3
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(SSLHandshakeException.class)
                    .hasRootCauseMessage("Received fatal alert: protocol_version");
        }
    }

    @Test
    void downstream_SuccessfulTlsWithProtocolsDenied(KafkaCluster cluster) {
        // Protocol we want to use
        AllowDeny<String> protocols = new AllowDeny<>(null, Set.of("TLSv1.2"));

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withProtocols(protocols)
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword(),
                                // Accepted Protocol matches what we want to use even with a denied protocol
                                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.3"))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstream_SuccessfulTlsWithProtocolsAllowed(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var brokerTrustPasswordProvider = constructPasswordProvider(InlinePassword.class, brokerTruststorePassword);

        // Protocol we want to use
        AllowDeny<String> protocols = new AllowDeny<>(List.of("TLSv1.2"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withStorePasswordProvider(brokerTrustPasswordProvider)
                        .endTrustStoreTrust()
                        .withProtocols(protocols)
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo")) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstream_UnsuccessfulTlsWithProtocolsAllowed(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var brokerTrustPasswordProvider = constructPasswordProvider(InlinePassword.class, brokerTruststorePassword);

        // Protocol we want to use
        AllowDeny<String> protocols = new AllowDeny<>(List.of("TLSv1.1"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withStorePasswordProvider(brokerTrustPasswordProvider)
                        .endTrustStoreTrust()
                        .withProtocols(protocols)
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo")) {
            // Upstream only wants us to use TLSv1.1
            assertThat(admin.describeCluster(new DescribeClusterOptions().timeoutMs(10_000)).clusterId())
                    .failsWithin(Duration.ofSeconds(30))
                    .withThrowableThat()
                    .withCauseInstanceOf(TimeoutException.class)
                    .havingCause()
                    .withMessageStartingWith("Timed out waiting for a node assignment.");
        }
    }

    @Test
    void downstream_SuccessfulTlsWithCipherSuitesAllowed(KafkaCluster cluster) {
        // Cipher we want to use
        AllowDeny<String> cipherSuites = new AllowDeny<>(List.of("TLS_CHACHA20_POLY1305_SHA256"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withCipherSuites(cipherSuites)
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword(),
                                // Accepted Cipher matches what we want to use
                                SslConfigs.SSL_CIPHER_SUITES_CONFIG, "TLS_CHACHA20_POLY1305_SHA256"))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();

            // verify that the admin is actually using the intended cipher
            assertThat(admin)
                    .extracting("instance")
                    .extracting("client")
                    .extracting("selector")
                    .extracting("channels", InstanceOfAssertFactories.map(String.class, Object.class))
                    .anySatisfy((id, channel) -> {
                        assertThat(channel)
                                .extracting("transportLayer")
                                .extracting("sslEngine", InstanceOfAssertFactories.type(SSLEngine.class))
                                .extracting(SSLEngine::getSession)
                                .extracting(SSLSession::getCipherSuite)
                                .isEqualTo("TLS_CHACHA20_POLY1305_SHA256");
                    });

        }
    }

    @Test
    void downstream_UnsuccessfulTlsWithCipherSuitesAllowed(KafkaCluster cluster) {
        // Cipher we want to use
        AllowDeny<String> cipherSuites = new AllowDeny<>(List.of("TLS_AES_128_GCM_SHA256"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withCipherSuites(cipherSuites)
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword(),
                                // Accepted Cipher doesn't match what we want to use
                                SslConfigs.SSL_CIPHER_SUITES_CONFIG, "TLS_CHACHA20_POLY1305_SHA256"))) {
            // Server will only allow us to use TLS_CHACHA20_POLY1305_SHA256 and we only want to use TLS_AES_128_GCM_SHA256
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(SSLHandshakeException.class)
                    .hasRootCauseMessage("Received fatal alert: handshake_failure");
        }
    }

    @Test
    void downstream_SuccessfulTlsWithCipherSuitesAllowedAndDenied(KafkaCluster cluster) {
        // Cipher we want to use
        AllowDeny<String> cipherSuites = new AllowDeny<>(List.of("TLS_CHACHA20_POLY1305_SHA256"), Set.of("TLS_AES_128_GCM_SHA256"));

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withCipherSuites(cipherSuites)
                                .endTls()
                                .build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword(),
                                // Accepted Cipher matches what we want to use
                                SslConfigs.SSL_CIPHER_SUITES_CONFIG, "TLS_CHACHA20_POLY1305_SHA256"))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstream_SuccessfulTlsWithCipherSuitesAllowed(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var brokerTrustPasswordProvider = constructPasswordProvider(InlinePassword.class, brokerTruststorePassword);

        // Cipher we want to use
        AllowDeny<String> cipherSuites = new AllowDeny<>(List.of("TLS_CHACHA20_POLY1305_SHA256"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withStorePasswordProvider(brokerTrustPasswordProvider)
                        .endTrustStoreTrust()
                        .withCipherSuites(cipherSuites)
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo")) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void upstream_UnsuccessfulTlsWithCipherSuitesAllowed(@Tls KafkaCluster cluster) {
        var bootstrapServers = cluster.getBootstrapServers();
        var brokerTruststore = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var brokerTruststorePassword = (String) cluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(brokerTruststore).isNotEmpty();
        assertThat(brokerTruststorePassword).isNotEmpty();

        var brokerTrustPasswordProvider = constructPasswordProvider(InlinePassword.class, brokerTruststorePassword);

        // Cipher we want to use upstream
        AllowDeny<String> upstreamCipherSuites = new AllowDeny<>(List.of("TLS_AES_128_WRONG_CIPHER"), null);

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .withNewTls()
                        .withNewTrustStoreTrust()
                        .withStoreFile(brokerTruststore)
                        .withStorePasswordProvider(brokerTrustPasswordProvider)
                        .endTrustStoreTrust()
                        .withCipherSuites(upstreamCipherSuites)
                        .endTls()
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo")) {
            // Upstream trying to use invalid cipher
            assertThat(admin.describeCluster(new DescribeClusterOptions().timeoutMs(10_000)).clusterId())
                    .failsWithin(Duration.ofSeconds(30))
                    .withThrowableThat()
                    .withCauseInstanceOf(TimeoutException.class)
                    .havingCause()
                    .withMessageStartingWith("Timed out waiting for a node assignment.");
        }
    }

    @Test
    void downstreamMutualTls_SuccessfulTlsClientAuthRequiredByDefault(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, null));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword(),
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void downstreamMutualTls_UnsuccessfulTlsClientAuthRequiredByDefault(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, null));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // Would need key information provided for mTLS to work here for TlsClientAuth.REQUIRED
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(SSLHandshakeException.class)
                    .hasRootCauseMessage("Received fatal alert: bad_certificate");
        }
    }

    @Test
    void downstreamMutualTls_SuccessfulTlsClientAuthRequired(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, TlsClientAuth.REQUIRED));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword(),
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void downstreamMutualTls_UnsuccessfulTlsClientAuthRequired(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, TlsClientAuth.REQUIRED));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // Would need key information provided for mTLS to work here for TlsClientAuth.REQUIRED
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(SSLHandshakeException.class)
                    .hasRootCauseMessage("Received fatal alert: bad_certificate");
        }
    }

    @Test
    void downstreamMutualTls_SuccessfulTlsClientAuthRequestedAndProvided(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, TlsClientAuth.REQUESTED));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword(),
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void downstreamMutualTls_SuccessfulTlsClientAuthRequestedAndNotProvided(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, TlsClientAuth.REQUESTED));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void downstreamMutualTls_SuccessfulTlsClientAuthNoneAndProvided(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, TlsClientAuth.NONE));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword(),
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    @Test
    void downstreamMutualTls_SuccessfulTlsClientAuthNoneAndNotProvided(KafkaCluster cluster) {
        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster, TlsClientAuth.NONE));
                var admin = tester.admin("demo",
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                SecurityProtocol.SSL.name,
                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword()))) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, TOPIC, 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }

    private ConfigurationBuilder constructMutualTlsBuilder(KafkaCluster cluster, TlsClientAuth tlsClientAuth) {

        return new ConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withNewTls()
                                .withNewKeyStoreKey()
                                .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                .endKeyStoreKey()
                                .withNewTrustStoreTrust()
                                .withNewServerOptionsTrust()
                                .withClientAuth(tlsClientAuth)
                                .endServerOptionsTrust()
                                .withStoreFile(proxyTrustStore.toAbsolutePath().toString())
                                .withNewInlinePasswordStoreProvider(clientCertGenerator.getPassword())
                                .endTrustStoreTrust()
                                .endTls()
                                .build())
                        .build());
    }

    private PasswordProvider constructPasswordProvider(Class<? extends PasswordProvider> providerClazz, String password) {
        if (providerClazz.equals(InlinePassword.class)) {
            return new InlinePassword(password);
        }
        else if (providerClazz.equals(FilePassword.class)) {
            return new FilePassword(writePasswordToFile(password));
        }
        else {
            throw new IllegalArgumentException("Unexpected provider class: " + providerClazz);
        }

    }

    @NonNull
    private String writePasswordToFile(String password) {
        try {
            File tmp = File.createTempFile("password", ".txt");
            tmp.deleteOnExit();
            makeFileOwnerReadWriteOnly(tmp);
            Files.writeString(tmp.toPath(), password);
            // remove write from owner
            assertThat(tmp.setWritable(false, true)).isTrue();
            return tmp.getAbsolutePath();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write password to file", e);
        }
    }

    private File writeTrustToTemporaryFile(List<X509Certificate> certificates) {
        try {
            var file = File.createTempFile("trust", ".pem");
            makeFileOwnerReadWriteOnly(file);
            file.deleteOnExit();
            var mimeLineEnding = new byte[]{ '\r', '\n' };

            try (var out = new FileOutputStream(file)) {
                certificates.forEach(c -> {
                    var encoder = Base64.getMimeEncoder();
                    try {
                        out.write("-----BEGIN CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                        out.write(mimeLineEnding);
                        out.write(encoder.encode(c.getEncoded()));
                        out.write(mimeLineEnding);
                        out.write("-----END CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                        out.write(mimeLineEnding);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    catch (CertificateEncodingException e) {
                        throw new RuntimeException(e);
                    }
                });
                assertThat(file.setWritable(false, true)).isTrue();
                return file;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write trust to temporary file", e);
        }
    }

    private void makeFileOwnerReadWriteOnly(File f) {
        // remove read/write from everyone
        assertThat(f.setReadable(false, false)).isTrue();
        assertThat(f.setWritable(false, false)).isTrue();
        // add read/write for owner
        assertThat(f.setReadable(true, true)).isTrue();
        assertThat(f.setWritable(true, true)).isTrue();
    }
}
