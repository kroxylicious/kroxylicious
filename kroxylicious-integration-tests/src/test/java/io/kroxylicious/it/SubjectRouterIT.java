/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import javax.net.ssl.KeyManagerFactory;
import javax.security.auth.x500.X500Principal;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import io.kroxylicious.filter.sasl.termination.SaslTermination;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TransportSubjectBuilderConfig;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsBuilder;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.router.subject.SubjectRouter;
import io.kroxylicious.router.subject.UserNameMatch;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileManager;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileService;
import io.kroxylicious.testing.filter.assertj.KafkaAssertions;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicNameMethodSource;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end tests for the Subject Router (design proposal kroxylicious/design#140), proving the
 * router works through the real proxy pipeline: routing an authenticated connection to the mapped
 * downstream cluster, rejecting unmapped/anonymous connections fail-closed, fanning out anonymous
 * {@code API_VERSIONS} pre-SASL, and applying per-route filters.
 *
 * <p>The SASL fan-out test uses two real (identically-versioned) Kafka clusters rather than mock
 * brokers advertising divergent version ranges: {@code SubjectRoutingHandlerTest} already exercises
 * the intersection maths (narrowed ranges, dropped keys, error handling) exhaustively at the unit
 * level. This IT's job is to prove the fan-out/route wiring works through the real SASL termination
 * and router pipeline, which a same-version intersection still demonstrates.</p>
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SubjectRouterIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String PROXY_ADDRESS = "localhost:9192";
    private static final String TOPIC = "subject-router-it-topic";

    @Name("clusterA")
    static KafkaCluster clusterA;
    @Name("clusterB")
    static KafkaCluster clusterB;

    @SuppressWarnings("unused") // topic creation managed by KafkaClusterExtension
    @Name("clusterA")
    @TopicNameMethodSource("fixedTopicName")
    static Topic clusterATopic;

    @SuppressWarnings("unused") // topic creation managed by KafkaClusterExtension
    @Name("clusterB")
    @TopicNameMethodSource("fixedTopicName")
    static Topic clusterBTopic;

    @SuppressWarnings("unused") // referenced by @TopicNameMethodSource
    static String fixedTopicName() {
        return TOPIC;
    }

    @TempDir
    Path certsDirectory;

    KeytoolCertificateGenerator proxyCertGenerator;
    KeytoolCertificateGenerator aliceCertGenerator;
    KeytoolCertificateGenerator carolCertGenerator;
    KeytoolCertificateGenerator eveCertGenerator;
    Path clientTrustStore;
    Path proxyTrustStore;
    String proxyTrustStorePassword;

    @BeforeEach
    void generateCertificates() throws Exception {
        proxyCertGenerator = new KeytoolCertificateGenerator();
        proxyCertGenerator.generateSelfSignedCertificateEntry("proxy@kroxylicious.io", "localhost", "KI", "kroxylicious.io", null, null, "US");
        clientTrustStore = certsDirectory.resolve("client.truststore.jks");
        proxyCertGenerator.generateTrustStore(proxyCertGenerator.getCertFilePath(), "proxy", clientTrustStore.toAbsolutePath().toString());

        aliceCertGenerator = new KeytoolCertificateGenerator();
        aliceCertGenerator.generateSelfSignedCertificateEntry("alice@kroxylicious.io", "alice", "Dev", "kroxylicious.io", null, null, "US");
        carolCertGenerator = new KeytoolCertificateGenerator();
        carolCertGenerator.generateSelfSignedCertificateEntry("carol@kroxylicious.io", "carol", "Dev", "kroxylicious.io", null, null, "US");
        eveCertGenerator = new KeytoolCertificateGenerator();
        eveCertGenerator.generateSelfSignedCertificateEntry("eve@kroxylicious.io", "eve", "Dev", "kroxylicious.io", null, null, "US");

        // one shared truststore on the proxy side, trusting all three client identities
        proxyTrustStore = certsDirectory.resolve("proxy.truststore.jks");
        var trustStoreHolder = new KeytoolCertificateGenerator();
        proxyTrustStorePassword = trustStoreHolder.getPassword();
        trustStoreHolder.generateTrustStore(aliceCertGenerator.getCertFilePath(), "alice", proxyTrustStore.toAbsolutePath().toString());
        trustStoreHolder.generateTrustStore(carolCertGenerator.getCertFilePath(), "carol", proxyTrustStore.toAbsolutePath().toString());
        trustStoreHolder.generateTrustStore(eveCertGenerator.getCertFilePath(), "eve", proxyTrustStore.toAbsolutePath().toString());
    }

    private static String principalNameOf(KeytoolCertificateGenerator certGenerator) throws Exception {
        try (InputStream in = Files.newInputStream(Path.of(certGenerator.getCertFilePath()))) {
            X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
            return cert.getSubjectX500Principal().getName(X500Principal.RFC1779, Map.of("1.2.840.113549.1.9.1", "emailAddress"));
        }
    }

    private Tls gatewayTls() {
        // @formatter:off
        return new TlsBuilder()
                .withNewKeyStoreKey()
                    .withStoreFile(proxyCertGenerator.getKeyStoreLocation())
                    .withStorePasswordProvider(new InlinePassword(proxyCertGenerator.getPassword()))
                .endKeyStoreKey()
                .withNewTrustStoreTrust()
                    .withNewServerOptionsTrust()
                        .withClientAuth(TlsClientAuth.REQUIRED)
                    .endServerOptionsTrust()
                    .withStoreFile(proxyTrustStore.toAbsolutePath().toString())
                    .withNewInlinePasswordStoreProvider(proxyTrustStorePassword)
                .endTrustStoreTrust()
                .build();
        // @formatter:on
    }

    private Map<String, Object> mtlsClientConfigs(KeytoolCertificateGenerator clientCertGenerator, String clientId) {
        return Map.of(
                CLIENT_ID_CONFIG, clientId,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, proxyCertGenerator.getPassword(),
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword());
    }

    /**
     * Builds a two-cluster/two-route SubjectRouter config, gated behind mTLS, mapping alice's and
     * carol's certificate principals to route-a/route-b respectively. eve is left unmapped
     * (no default route) so her connections are rejected fail-closed.
     *
     * @param routeAFilterNames filter definition names to attach to route-a only (empty for none)
     */
    private ConfigurationBuilder mtlsRoutingConfig(List<NamedFilterDefinition> filterDefs, List<String> routeAFilterNames)
            throws Exception {
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, routeAFilterNames, new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));

        var selectorConfig = new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("route-a", List.of(principalNameOf(aliceCertGenerator))),
                        new UserNameMatch.Mapping("route-b", List.of(principalNameOf(carolCertGenerator)))),
                null);
        var routerDef = new RouterDefinition("subject-router", SubjectRouter.class.getName(),
                new SubjectRouter.Config("UserNameMatch", selectorConfig), List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "subject-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).withTls(Optional.of(gatewayTls())).build())
                .withSubjectBuilder(new TransportSubjectBuilderConfig(MyTransportSubjectBuilderService.class.getName(),
                        new MyTransportSubjectBuilderService.Config(0, true)))
                .build();

        var builder = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA, clusterDefB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
        filterDefs.forEach(builder::addToFilterDefinitions);
        return builder;
    }

    private List<ConsumerRecord<String, String>> consumeFrom(KafkaCluster cluster, String groupId) {
        Map<String, Object> configs = new HashMap<>(Map.of(
                GROUP_ID_CONFIG, groupId,
                AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(cluster, configs))) {
            consumer.subscribe(List.of(TOPIC));
            return StreamSupport.stream(consumer.poll(Duration.ofSeconds(10)).records(TOPIC).spliterator(), false).toList();
        }
    }

    private Map<String, Object> consumerProps(KafkaCluster cluster, Map<String, Object> extra) {
        Map<String, Object> props = new HashMap<>(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
        props.putAll(extra);
        return props;
    }

    @Test
    void mtlsRoutesConnectionsByPrincipal() throws Exception {
        // Given
        var config = mtlsRoutingConfig(List.of(), List.of());

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                Producer<String, String> aliceProducer = tester.producer(mtlsClientConfigs(aliceCertGenerator, "alice-producer"));
                Producer<String, String> carolProducer = tester.producer(mtlsClientConfigs(carolCertGenerator, "carol-producer"))) {
            assertThat(aliceProducer.send(new ProducerRecord<>(TOPIC, "t1-alice-key", "t1-alice-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
            assertThat(carolProducer.send(new ProducerRecord<>(TOPIC, "t1-carol-key", "t1-carol-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
        }

        // Then
        assertThat(consumeFrom(clusterA, "t1-verify-a"))
                .extracting(ConsumerRecord::key)
                .contains("t1-alice-key")
                .doesNotContain("t1-carol-key");
        assertThat(consumeFrom(clusterB, "t1-verify-b"))
                .extracting(ConsumerRecord::key)
                .contains("t1-carol-key")
                .doesNotContain("t1-alice-key");
    }

    @Test
    void unmappedPrincipalRejectedFailClosed() throws Exception {
        // Given: no default route, and eve's certificate is not mapped to anything.
        // A real KafkaProducer can't observe this cleanly for the same reason as the anonymous
        // case: METADATA has no per-request slot to carry an auth-failure code back to a
        // topic-less/bootstrap request, so the router's rejection is verified precisely instead,
        // via a raw mTLS client, by checking the connection is closed after the request.
        var config = mtlsRoutingConfig(List.of(), List.of());
        var eveSslContext = clientSslContext(eveCertGenerator);

        // When / Then
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                var client = new KafkaClient("localhost", 9192, eveSslContext)) {
            client.getSync(new Request(ApiKeys.METADATA, (short) 9, "eve-client", new MetadataRequestData()));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(client.isOpen()).isFalse());
        }
    }

    private SslContext clientSslContext(KeytoolCertificateGenerator clientCertGenerator) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(clientCertGenerator.getKeyStoreLocation()))) {
            keyStore.load(in, clientCertGenerator.getPassword().toCharArray());
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, clientCertGenerator.getPassword().toCharArray());
        return SslContextBuilder.forClient()
                .keyManager(keyManagerFactory)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
    }

    @Test
    void anonymousNonApiVersionsRequestRejectedFailClosed() {
        // Given: no TLS, no subject builder configured -> every connection is anonymous.
        // A real KafkaProducer can't observe this cleanly: METADATA has no per-request slot to
        // carry an auth-failure code back to a topic-less/bootstrap request, so a real client just
        // sees an empty broker list followed by a closed connection and retries until its own
        // metadata timeout expires. The router's own rejection is verified precisely instead, via
        // the raw protocol client, by checking the connection is closed after the request.
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);
        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var selectorConfig = new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("route-a", List.of("CN=alice"))), null);
        var routerDef = new RouterDefinition("subject-router", SubjectRouter.class.getName(),
                new SubjectRouter.Config("UserNameMatch", selectorConfig), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "subject-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA, clusterDefB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        // When / Then
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {
            client.getSync(new Request(ApiKeys.METADATA, (short) 9, "anon-client", new MetadataRequestData()));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(client.isOpen()).isFalse());
        }
    }

    @Test
    void perRouteFilterOnlyAffectsThatRoutesTraffic() throws Exception {
        // Given: ClientAuthAwareLawyer attached to route-a only
        var lawyer = new NamedFilterDefinitionBuilder(ClientAuthAwareLawyer.class.getName(), ClientAuthAwareLawyer.class.getName()).build();
        var config = mtlsRoutingConfig(List.of(lawyer), List.of(lawyer.name()));

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                Producer<String, String> aliceProducer = tester.producer(mtlsClientConfigs(aliceCertGenerator, "alice-producer"));
                Producer<String, String> carolProducer = tester.producer(mtlsClientConfigs(carolCertGenerator, "carol-producer"))) {
            assertThat(aliceProducer.send(new ProducerRecord<>(TOPIC, "t5-alice-key", "t5-alice-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
            assertThat(carolProducer.send(new ProducerRecord<>(TOPIC, "t5-carol-key", "t5-carol-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
        }

        // Then: only route-a's (alice's) record carries the marker header
        var aliceRecord = consumeFrom(clusterA, "t5-verify-a").stream()
                .filter(r -> "t5-alice-key".equals(r.key())).findFirst().orElseThrow();
        assertThat(aliceRecord).asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                .headers().singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_TLS_IS_PRESENT)
                .value().containsExactly((byte) 1);

        var carolRecord = consumeFrom(clusterB, "t5-verify-b").stream()
                .filter(r -> "t5-carol-key".equals(r.key())).findFirst().orElseThrow();
        assertThat(carolRecord.headers().lastHeader(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_TLS_IS_PRESENT)).isNull();
    }

    @Test
    void saslPreAuthApiVersionsFanOutThenRoutesByAuthorizationId(@TempDir Path tempDir) throws Exception {
        // Given
        String username = "alice";
        String password = "alice-secret-password-123";
        String keystorePassword = "keystore-password-secret-456";
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, keystorePassword);
        credentialManager.addUser(keystorePath, keystorePassword, username, password, ScramMechanism.SCRAM_SHA_256);

        NamedFilterDefinition saslTermination = new NamedFilterDefinitionBuilder(SaslTermination.class.getSimpleName(), SaslTermination.class.getName())
                .withConfig("mechanisms", List.of(Map.of(
                        "mechanism", "SCRAM-SHA-256",
                        "credentialStore", ScramCredentialFileService.class.getName(),
                        "credentialStoreConfig", Map.of("file", keystorePath.toString(), "filePassword", Map.of("password", keystorePassword)))))
                .build();

        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);
        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var selectorConfig = new UserNameMatch.Config(List.of(new UserNameMatch.Mapping("route-a", List.of(username))), null);
        var routerDef = new RouterDefinition("subject-router", SubjectRouter.class.getName(),
                new SubjectRouter.Config("UserNameMatch", selectorConfig), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "subject-router"))
                .addToFilters(saslTermination.name())
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA, clusterDefB)
                .addToFilterDefinitions(saslTermination)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password);
        Map<String, Object> clientConfigs = new HashMap<>(Map.of(
                CLIENT_ID_CONFIG, "sasl-fanout-client",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_256.mechanismName(),
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));

        // When: a fresh KafkaProducer always negotiates API_VERSIONS (anonymously, pre-SASL) before
        // authenticating, so a successful send exercises the fan-out/intersection path as well as
        // the authenticated forward.
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                Producer<String, String> producer = tester.producer(clientConfigs)) {
            assertThat(producer.send(new ProducerRecord<>(TOPIC, "t4-sasl-key", "t4-sasl-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
        }

        // Then: routed to cluster-a, as mapped for "alice"
        assertThat(consumeFrom(clusterA, "t4-verify-a"))
                .extracting(ConsumerRecord::key)
                .contains("t4-sasl-key");
    }
}
