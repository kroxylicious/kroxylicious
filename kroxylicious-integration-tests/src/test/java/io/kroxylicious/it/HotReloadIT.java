/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.it.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeystoreManager;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration tests for {@link KafkaProxy#reconfigure(Configuration)}
 */
class HotReloadIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotReloadIT.class);

    private static final String VC_KEEP_NAME = "vc-keep";
    private static final String VC_REMOVE_NAME = "vc-remove";

    private static final int SHARED_SNI_PORT = KroxyliciousConfigUtils.DEFAULT_PROXY_BOOTSTRAP.port();
    private static final String SNI_BASE_DOMAIN = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".hotreload");
    private static final String VC_KEEP_BOOTSTRAP = "bootstrap-keep" + SNI_BASE_DOMAIN + ":" + SHARED_SNI_PORT;
    private static final String VC_REMOVE_BOOTSTRAP = "bootstrap-remove" + SNI_BASE_DOMAIN + ":" + SHARED_SNI_PORT;
    private static final String VC_KEEP_BROKER_PATTERN = "broker-$(nodeId)-keep" + SNI_BASE_DOMAIN;
    private static final String VC_REMOVE_BROKER_PATTERN = "broker-$(nodeId)-remove" + SNI_BASE_DOMAIN;

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration PRODUCE_CONSUME_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration REJECTION_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void shouldStopRemovedVcButContinueServingOthersEndToEnd(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Wildcard cert covering both VCs' SNI hostnames (both end in SNI_BASE_DOMAIN).
        KeystoreTrustStorePair certs = buildKeystoreTrustStorePair("*" + SNI_BASE_DOMAIN);
        var twoVcConfig = buildConfig(cluster, certs, true);
        var oneVcConfig = buildConfig(cluster, certs, false);

        // Tester builder so we can register the truststore — required for the default client
        // configuration to do SSL handshakes against the SNI-addressed VCs.
        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(twoVcConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder)
                .setTrustStoreLocation(certs.clientTrustStore())
                .setTrustStorePassword(certs.password())
                .createDefaultKroxyliciousTester()) {

            String topic = tester.createTopic(VC_KEEP_NAME);

            // Phase 1: both SNI-addressed VCs serve produce + consume on the shared port.
            LOGGER.info("Phase 1: producing + consuming through both SNI-addressed VCs");
            assertProduceConsumeRoundTrip(tester, VC_KEEP_NAME, topic, "phase1-keep");
            assertProduceConsumeRoundTrip(tester, VC_REMOVE_NAME, topic, "phase1-remove");

            // Phase 2: reconfigure removes vc-remove. The acceptor channel stays alive
            // (vc-keep still needs it); vc-remove transitions to STOPPED.
            LOGGER.info("Phase 2: reconfiguring to remove '{}'", VC_REMOVE_NAME);
            var result = tester.reconfigure(oneVcConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean pure-remove")
                    .isFalse();

            // Phase 3: vc-keep continues to serve on the same port + cert. An unaffected
            // VC is genuinely undisturbed by the reconfigure.
            LOGGER.info("Phase 3: verifying '{}' still serves produce + consume", VC_KEEP_NAME);
            assertProduceConsumeRoundTrip(tester, VC_KEEP_NAME, topic, "phase3-keep");

            // Phase 4: new connections with vc-remove's SNI hostname are rejected. TLS
            // handshake succeeds (cert is wildcard, covers both hostnames), but the Kafka
            // session is closed because the VC behind that SNI binding is now STOPPED —
            // the SERVING-state guard rejects registration.
            LOGGER.info("Phase 4: verifying '{}' no longer accepts traffic on its SNI hostname", VC_REMOVE_NAME);
            assertProducerFailure(tester, VC_REMOVE_NAME, topic);
        }
    }

    private static Configuration buildConfig(KafkaCluster cluster, KeystoreTrustStorePair certs, boolean includeRemoveVc) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(buildSniVirtualCluster(cluster, certs, VC_KEEP_NAME, VC_KEEP_BOOTSTRAP, VC_KEEP_BROKER_PATTERN));
        if (includeRemoveVc) {
            builder.addToVirtualClusters(buildSniVirtualCluster(cluster, certs, VC_REMOVE_NAME, VC_REMOVE_BOOTSTRAP, VC_REMOVE_BROKER_PATTERN));
        }
        return builder.build();
    }

    private static VirtualCluster buildSniVirtualCluster(KafkaCluster cluster,
                                                         KeystoreTrustStorePair certs,
                                                         String name,
                                                         String bootstrap,
                                                         String brokerPattern) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder(bootstrap, brokerPattern)
                        .withNewTls()
                        .withNewKeyStoreKey()
                        .withStoreFile(certs.brokerKeyStore())
                        .withNewInlinePasswordStoreProvider(certs.password())
                        .endKeyStoreKey()
                        .endTls()
                        .build())
                .build();
    }

    // Per-record produce: batch.size=1 + linger.ms=0 forces the producer to issue one
    // ProduceRequest per record rather than coalescing records into a single batched
    // request.
    private static final Map<String, Object> PER_RECORD_PRODUCER_CONFIG = Map.of(
            ProducerConfig.BATCH_SIZE_CONFIG, 1,
            ProducerConfig.LINGER_MS_CONFIG, 0,
            ProducerConfig.ACKS_CONFIG, "all");

    /**
     * Produce a random number of records (50-100) via {@code tester.producer(vc)}
     * configured with {@code batch.size=1, linger.ms=0} so each record results in its
     * own {@code ProduceRequest} — the proxy sees N round-trips rather than one batched
     * request. Then consume them back via {@code tester.consumer(vc)}. Records are keyed
     * {@code marker-0}..{@code marker-N-1} so the consumer can isolate this round-trip's
     * records from earlier phases' records that share the same topic.
     *
     * <p>Asserts the consumer observes exactly {@code N} unique keys carrying this call's
     * {@code marker} — proving the full proxy I/O round-trip is working and that no
     * records are dropped mid-stream across many independent produce requests.
     */
    private static void assertProduceConsumeRoundTrip(KroxyliciousTester tester, String vc, String topic, String marker) throws Exception {
        int messageCount = ThreadLocalRandom.current().nextInt(50, 101);
        LOGGER.info("Producing {} records via VC '{}' (marker='{}') with per-record requests", messageCount, vc, marker);

        var sendFutures = new ArrayList<Future<RecordMetadata>>(messageCount);
        try (var producer = tester.producer(vc, PER_RECORD_PRODUCER_CONFIG)) {
            for (int i = 0; i < messageCount; i++) {
                sendFutures.add(producer.send(new ProducerRecord<>(topic, marker + "-" + i, marker + "-value-" + i)));
            }
            for (Future<RecordMetadata> f : sendFutures) {
                f.get(PRODUCE_CONSUME_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            }
        }

        try (var consumer = tester.consumer(vc, Map.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_ID_CONFIG, "hotreload-it-consumer-" + marker))) {
            consumer.subscribe(List.of(topic));
            var seenKeys = new HashSet<String>();
            String keyPrefix = marker + "-";
            long deadline = System.currentTimeMillis() + PRODUCE_CONSUME_TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline && seenKeys.size() < messageCount) {
                var batch = consumer.poll(Duration.ofMillis(500));
                for (var record : batch) {
                    if (record.key() != null && record.key().startsWith(keyPrefix)) {
                        seenKeys.add(record.key());
                    }
                }
            }
            assertThat(seenKeys)
                    .as("consumer should have read all %d records (marker='%s') via VC '%s' within %s",
                            messageCount, marker, vc, PRODUCE_CONSUME_TIMEOUT)
                    .hasSize(messageCount);
        }
    }

    /**
     * Open a fresh producer against the removed VC and verify it cannot deliver a record.
     * Uses aggressively short timeouts so the failure surfaces within
     * {@link #REJECTION_TIMEOUT} rather than the default Kafka client delivery timeout.
     */
    private static void assertProducerFailure(KroxyliciousTester tester, String vc, String topic) {
        try (var producer = tester.producer(vc, shortTimeoutProducerConfig())) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(topic, "should-fail", "should-fail-value"))
                    .get(REJECTION_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .as("producer should fail to deliver to the removed VC '%s'", vc)
                    .isInstanceOf(ExecutionException.class);
        }
    }

    private static Map<String, Object> shortTimeoutProducerConfig() {
        return Map.of(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5_000,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000,
                ProducerConfig.RETRIES_CONFIG, 1,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 100,
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
    }

    private record KeystoreTrustStorePair(String brokerKeyStore, String clientTrustStore, String password) {}

    private static KeystoreTrustStorePair buildKeystoreTrustStorePair(String domain) throws Exception {
        var keystoreManager = new KeystoreManager();
        String dn = keystoreManager.buildDistinguishedName("test@kroxylicious.io", domain, "KI", "kroxylicious.io", null, null, "US");
        var bundle = keystoreManager.createSelfSignedCertificate(
                keystoreManager.newCertificateBuilder(dn));
        Path keystorePath = keystoreManager.generateCertificateFile(bundle);
        String password = keystoreManager.getPassword(keystorePath);
        // The generated JKS contains both the private key entry and the CA cert,
        // so the same file serves as both the proxy keystore and the client truststore.
        String keystore = keystorePath.toAbsolutePath().toString();
        return new KeystoreTrustStorePair(keystore, keystore, password);
    }
}
