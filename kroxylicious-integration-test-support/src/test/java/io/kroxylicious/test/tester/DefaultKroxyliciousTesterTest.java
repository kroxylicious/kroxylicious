/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.matcher.AssertionMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterListenerBuilder;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_LISTENER_NAME;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_PROXY_BOOTSTRAP;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultKroxyliciousTesterTest {

    private static final String VIRTUAL_CLUSTER_A = "clusterA";
    private static final String VIRTUAL_CLUSTER_B = "clusterB";
    private static final String VIRTUAL_CLUSTER_C = "clusterC";
    private static final String EXCEPTION_MESSAGE = "KaBOOM!!";
    private static final String DEFAULT_CLUSTER = "demo";
    private static final String TLS_CLUSTER = "secureCluster";
    public static final int TOPIC_COUNT = 5;
    public static final String CUSTOM_LISTENER_NAME = "listener";
    String backingCluster = "broker01.example.com:9090";

    @Mock(strictness = LENIENT)
    DefaultKroxyliciousTester.ClientFactory clientFactory;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClients;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClientsA;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClientsB;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClientsC;

    @Mock
    Admin admin;

    private final MockProducer<String, String> producer = new MockProducer<>();

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    @BeforeEach
    void setUp() {
        when(clientFactory.build(eq(new ListenerId(DEFAULT_VIRTUAL_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap())).thenReturn(kroxyliciousClients);
        when(clientFactory.build(eq(new ListenerId(DEFAULT_VIRTUAL_CLUSTER, CUSTOM_LISTENER_NAME)), anyMap())).thenReturn(kroxyliciousClients);
        when(clientFactory.build(eq(new ListenerId(TLS_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap())).thenReturn(kroxyliciousClients);
        when(clientFactory.build(eq(new ListenerId(VIRTUAL_CLUSTER_A, DEFAULT_LISTENER_NAME)), anyMap())).thenReturn(kroxyliciousClientsA);
        when(clientFactory.build(eq(new ListenerId(VIRTUAL_CLUSTER_B, DEFAULT_LISTENER_NAME)), anyMap())).thenReturn(kroxyliciousClientsB);
        when(clientFactory.build(eq(new ListenerId(VIRTUAL_CLUSTER_C, DEFAULT_LISTENER_NAME)), anyMap())).thenReturn(kroxyliciousClientsC);
        when(kroxyliciousClients.admin()).thenReturn(admin);
        when(kroxyliciousClients.producer()).thenReturn(producer);
        when(kroxyliciousClients.consumer()).thenReturn(consumer);
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.admin(DEFAULT_CLUSTER);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForVirtualClusterAndDefaultListener() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.admin(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForVirtualClusterAndCustomListener() {
        // Given
        try (var tester = buildMultiListenerTester()) {

            // When
            tester.admin(DEFAULT_CLUSTER, CUSTOM_LISTENER_NAME);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, CUSTOM_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForDefaultVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.admin();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.producer(DEFAULT_CLUSTER);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForDefaultVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.producer();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.consumer(DEFAULT_CLUSTER);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).consumer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForDefaultVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.consumer();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(new ListenerId(DEFAULT_CLUSTER, DEFAULT_LISTENER_NAME)), anyMap());
            verify(kroxyliciousClients).consumer();
        }
    }

    @Test
    void closingTesterShouldCloseClients() {
        // Given
        try (var tester = buildDefaultTester()) {
            tester.consumer();

            // When
            tester.close();

            // Then
            verify(kroxyliciousClients).close();
        }
    }

    @Test
    void shouldKeepClosingClientsWhenOneFails() {
        // Given
        try (var tester = buildTester()) {
            tester.admin(VIRTUAL_CLUSTER_A);
            tester.admin(VIRTUAL_CLUSTER_B);
            tester.admin(VIRTUAL_CLUSTER_C);
            // The doNothing is required so the try-with-resources block completes successfully
            doThrow(new IllegalStateException(EXCEPTION_MESSAGE)).doNothing().when(kroxyliciousClientsA).close();

            // When
            try {
                tester.close();
                fail("Expected tester to re-throw");
            }
            catch (RuntimeException re) {
                // not my problem
            }

            // Then
            verify(kroxyliciousClientsA).close();
            verify(kroxyliciousClientsB).close();
            verify(kroxyliciousClientsC).close();
        }
    }

    @Test
    void shouldPropagateExceptionsOnClose() {
        // Given
        try (var tester = buildTester()) {
            tester.admin(VIRTUAL_CLUSTER_A);
            // The doNothing is required so the try-with-resources block completes successfully
            doThrow(new IllegalStateException(EXCEPTION_MESSAGE)).doNothing().when(kroxyliciousClientsA).close();

            // When
            // Then
            assertThatThrownBy(tester::close)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(EXCEPTION_MESSAGE);
        }
    }

    @Test
    void shouldConfigureConsumerForTls(@TempDir Path certsDirectory) throws IOException {
        // Given
        final String certFilePath = certsDirectory.resolve(Path.of("cert-file")).toAbsolutePath().toString();
        final String trustStorePath = certsDirectory.resolve(Path.of("trust-store")).toAbsolutePath().toString();
        var keytoolCertificateGenerator = new KeytoolCertificateGenerator(certFilePath, trustStorePath);
        try (var tester = buildSecureTester(keytoolCertificateGenerator)) {

            // When
            tester.consumer(TLS_CLUSTER);
            tester.producer(TLS_CLUSTER);
            tester.admin(TLS_CLUSTER);

            // Then

            verify(clientFactory).build(eq(new ListenerId(TLS_CLUSTER, DEFAULT_LISTENER_NAME)), argThat(assertSslConfiguration(trustStorePath)));
        }
    }

    @Test
    void shouldConfigureProducerForTls(@TempDir Path certsDirectory) throws IOException {
        // Given
        final String certFilePath = certsDirectory.resolve(Path.of("cert-file")).toAbsolutePath().toString();
        final String trustStorePath = certsDirectory.resolve(Path.of("trust-store")).toAbsolutePath().toString();
        var keytoolCertificateGenerator = new KeytoolCertificateGenerator(certFilePath, trustStorePath);
        try (var tester = buildSecureTester(keytoolCertificateGenerator)) {

            // When
            tester.producer(TLS_CLUSTER);

            // Then
            verify(clientFactory).build(eq(new ListenerId(TLS_CLUSTER, DEFAULT_LISTENER_NAME)), argThat(assertSslConfiguration(trustStorePath)));
        }
    }

    @Test
    void shouldConfigureAdminForTls(@TempDir Path certsDirectory) throws IOException {
        // Given
        final String certFilePath = certsDirectory.resolve(Path.of("cert-file")).toAbsolutePath().toString();
        final String trustStorePath = certsDirectory.resolve(Path.of("trust-store")).toAbsolutePath().toString();
        var keytoolCertificateGenerator = new KeytoolCertificateGenerator(certFilePath, trustStorePath);
        try (var tester = buildSecureTester(keytoolCertificateGenerator)) {

            // When
            tester.admin(TLS_CLUSTER);

            // Then
            verify(clientFactory).build(eq(new ListenerId(TLS_CLUSTER, DEFAULT_LISTENER_NAME)), argThat(assertSslConfiguration(trustStorePath)));
        }
    }

    @Test
    void shouldCreateSingleTopic() {
        // Given
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        try (KroxyliciousTester tester = buildDefaultTester()) {

            // When
            final String actualTopicName = tester.createTopic(DEFAULT_CLUSTER);

            // Then
            verify(admin).createTopics(argThat(topics -> assertThat(topics).hasSize(1)));
            assertThat(actualTopicName).isNotBlank();
        }
    }

    @Test
    void shouldCreateMultipleTopics() {
        // Given
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        try (KroxyliciousTester tester = buildDefaultTester()) {

            // When
            final Set<String> actualTopicName = tester.createTopics(DEFAULT_CLUSTER, TOPIC_COUNT);

            // Then
            verify(admin).createTopics(argThat(topics -> assertThat(topics).hasSize(TOPIC_COUNT)));
            assertThat(actualTopicName).isNotEmpty().hasSize(TOPIC_COUNT);
        }
    }

    @Test
    void shouldTrackTopicsForCluster() {
        // Given
        final Admin adminB = mock(Admin.class);
        allowCreateTopic(kroxyliciousClientsA, admin);
        allowCreateTopic(kroxyliciousClientsB, adminB);

        try (KroxyliciousTester tester = buildTester()) {
            tester.createTopics(VIRTUAL_CLUSTER_A, TOPIC_COUNT);

            // When
            tester.createTopics(VIRTUAL_CLUSTER_B, TOPIC_COUNT);

            // Then
            verify(admin).createTopics(argThat(topics -> assertThat(topics).hasSize(TOPIC_COUNT)));
            verify(adminB).createTopics(argThat(topics -> assertThat(topics).hasSize(TOPIC_COUNT)));
        }
    }

    @Test
    void shouldDeleteTopicsForCluster() {
        // Given
        allowCreateTopic(kroxyliciousClientsA, admin);
        final DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(admin.deleteTopics(anyCollection())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        try (KroxyliciousTester tester = buildTester()) {
            tester.createTopics(VIRTUAL_CLUSTER_A, TOPIC_COUNT);

            // When
            tester.deleteTopics(VIRTUAL_CLUSTER_A);

            // Then
            verify(admin).deleteTopics(DefaultKroxyliciousTesterTest.<Collection<String>> argThat(topics -> assertThat(topics).hasSize(TOPIC_COUNT)));
        }
    }

    @Test
    void shouldNotThrowWhenNoTopicsCreated() {
        // Given
        when(kroxyliciousClientsA.admin()).thenReturn(admin);

        try (KroxyliciousTester tester = buildTester()) {
            // When
            tester.deleteTopics(VIRTUAL_CLUSTER_A);

            // Then
            // We can't use verifyNoInteractions or verifyNoMoreInteractions here as the try-with-resource block will trigger admin.close()
            verify(admin, times(0)).deleteTopics(anyCollection());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldPropagateErrorsCreatingTopics() throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        when(admin.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(kafkaFuture);
        when(kafkaFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new ExecutionException("Kafka is nae working pal", new KafkaException("KaBoom")));

        try (KroxyliciousTester tester = buildDefaultTester()) {

            // When
            assertThatThrownBy(() -> tester.createTopic(DEFAULT_CLUSTER))
                    // Then
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Failed to create topics on " + DEFAULT_CLUSTER)
                    .hasCauseInstanceOf(KafkaException.class);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldPropagateTimeoutCreatingTopics() throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        when(admin.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(kafkaFuture);
        when(kafkaFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("Kafka fell asleep"));

        try (KroxyliciousTester tester = buildDefaultTester()) {

            // When
            assertThatThrownBy(() -> tester.createTopic(DEFAULT_CLUSTER))
                    // Then
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Timed out creating topics on " + DEFAULT_CLUSTER);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldPropagateErrorsDeletingTopics() throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        allowCreateTopic(kroxyliciousClientsA, admin);

        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        final DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(admin.deleteTopics(anyCollection())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(kafkaFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new ExecutionException("Kafka is nae working pal", new KafkaException("KaBoom")));

        try (KroxyliciousTester tester = buildTester()) {
            tester.createTopics(VIRTUAL_CLUSTER_A, TOPIC_COUNT);

            // When
            assertThatThrownBy(() -> tester.deleteTopics(VIRTUAL_CLUSTER_A))
                    // Then
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Failed to delete topics on " + VIRTUAL_CLUSTER_A)
                    .hasCauseInstanceOf(KafkaException.class);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldPropagateTimeoutDeletingTopics() throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        allowCreateTopic(kroxyliciousClientsA, admin);

        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        final DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(admin.deleteTopics(anyCollection())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(kafkaFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("Kafka fell asleep"));

        try (KroxyliciousTester tester = buildTester()) {
            tester.createTopics(VIRTUAL_CLUSTER_A, TOPIC_COUNT);

            // When
            assertThatThrownBy(() -> tester.deleteTopics(VIRTUAL_CLUSTER_A))
                    // Then
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Timed out deleting topics on " + VIRTUAL_CLUSTER_A);
        }
    }

    @Test
    void shouldFailIfAdminHttpNotConfigured() {
        try (var tester = buildDefaultTester()) {
            assertThatThrownBy(tester::getAdminHttpClient)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("admin http interface not configured");
        }
    }

    @Test
    void shouldExposeMetrics() {
        var proxy = proxy(backingCluster).withNewAdminHttp()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endAdminHttp();
        var testerBuilder = new KroxyliciousTesterBuilder()
                .setConfigurationBuilder(proxy)
                .setKroxyliciousFactory(DefaultKroxyliciousTester::spawnProxy);

        try (var tester = (KroxyliciousTester) testerBuilder
                .createDefaultKroxyliciousTester()) {
            var adminHttpClient = tester.getAdminHttpClient();
            assertThat(adminHttpClient).isNotNull();
            var metrics = adminHttpClient.scrapeMetrics();
            assertThat(metrics).hasSizeGreaterThan(0);
        }
    }

    private void allowCreateTopic(KroxyliciousClients kroxyliciousClients, Admin admin) {
        when(kroxyliciousClients.admin()).thenReturn(admin);
        final CreateTopicsResult createTopicsResultA = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(createTopicsResultA);
        when(createTopicsResultA.all()).thenReturn(KafkaFuture.completedFuture(null));
    }

    @NonNull
    private static Consumer<Map<String, Object>> assertSslConfiguration(String trustStorePath) {
        return actual -> assertThat(actual)
                .contains(
                        Map.entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name()),
                        Map.entry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath))
                .containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
    }

    @NonNull
    private KroxyliciousTester buildDefaultTester() {
        return new KroxyliciousTesterBuilder().setConfigurationBuilder(proxy(backingCluster))
                .setKroxyliciousFactory(DefaultKroxyliciousTester::spawnProxy)
                .setClientFactory(clientFactory)
                .createDefaultKroxyliciousTester();
    }

    @NonNull
    private KroxyliciousTester buildMultiListenerTester() {
        final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        String virtualClusterName = DEFAULT_VIRTUAL_CLUSTER;
        var vcb = new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(backingCluster)
                .endTargetCluster()
                .addToListeners(DEFAULT_LISTENER_NAME, new VirtualClusterListenerBuilder()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder(PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                                        .withConfig("bootstrapAddress", new HostPort(DEFAULT_PROXY_BOOTSTRAP.host(), DEFAULT_PROXY_BOOTSTRAP.port()))
                                        .build())
                        .build())
                .addToListeners(CUSTOM_LISTENER_NAME, new VirtualClusterListenerBuilder()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder(PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                                        .withConfig("bootstrapAddress", new HostPort(DEFAULT_PROXY_BOOTSTRAP.host(), DEFAULT_PROXY_BOOTSTRAP.port() + 10))
                                        .build())
                        .build());
        configurationBuilder
                .addToVirtualClusters(virtualClusterName, vcb.build());
        return new KroxyliciousTesterBuilder().setConfigurationBuilder(configurationBuilder)
                .setKroxyliciousFactory(DefaultKroxyliciousTester::spawnProxy)
                .setClientFactory(clientFactory)
                .createDefaultKroxyliciousTester();
    }

    @NonNull
    private KroxyliciousTester buildSecureTester(KeytoolCertificateGenerator keytoolCertificateGenerator) {
        generateSecurityCert(keytoolCertificateGenerator);
        final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        var vcb = new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(backingCluster)
                .endTargetCluster()
                .addToListeners(DEFAULT_LISTENER_NAME, new VirtualClusterListenerBuilder()
                        .withNewTls()
                        .withNewKeyStoreKey()
                        .withStoreFile(keytoolCertificateGenerator.getKeyStoreLocation())
                        .withNewInlinePasswordStoreProvider(keytoolCertificateGenerator.getPassword())
                        .endKeyStoreKey()
                        .endTls()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder(PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                                        .withConfig("bootstrapAddress", DEFAULT_PROXY_BOOTSTRAP).build())
                        .build());
        configurationBuilder
                .addToVirtualClusters(TLS_CLUSTER, vcb.build());

        return new KroxyliciousTesterBuilder().setConfigurationBuilder(configurationBuilder)
                .setTrustStoreLocation(keytoolCertificateGenerator.getTrustStoreLocation())
                .setTrustStorePassword(keytoolCertificateGenerator.getPassword())
                .setKroxyliciousFactory(DefaultKroxyliciousTester::spawnProxy)
                .setClientFactory(clientFactory)
                .createDefaultKroxyliciousTester();
    }

    private static void generateSecurityCert(KeytoolCertificateGenerator keytoolCertificateGenerator) {
        try {
            keytoolCertificateGenerator.generateSelfSignedCertificateEntry("webmaster@example.com", "example.com", "Engineering", "kroxylicious.io", null, null, "NZ");
        }
        catch (GeneralSecurityException | IOException e) {
            fail("unable to generate security certificate", e);
        }
    }

    private KroxyliciousTester buildTester() {
        return new KroxyliciousTesterBuilder()
                .setConfigurationBuilder(proxy(backingCluster, VIRTUAL_CLUSTER_A, VIRTUAL_CLUSTER_B, VIRTUAL_CLUSTER_C))
                .setKroxyliciousFactory(DefaultKroxyliciousTester::spawnProxy).setClientFactory(clientFactory)
                .createDefaultKroxyliciousTester();
    }

    public static <T> T argThat(Consumer<T> assertions) {
        return MockitoHamcrest.argThat(new AssertionMatcher<>() {
            @Override
            public void assertion(T actual) throws AssertionError {
                assertions.accept(actual);
            }
        });
    }
}
