/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration tests for {@link KafkaProxy#reconfigure(Configuration)}
 */
@ExtendWith(KafkaClusterExtension.class)
class HotReloadIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotReloadIT.class);

    private static final String VC_KEEP_NAME = "vc-keep";
    private static final String VC_REMOVE_NAME = "vc-remove";

    // KroxyliciousConfigUtils.proxy(srvs, "vc1", "vc2") spaces VCs at the default bootstrap
    // port + i*10 — i.e. vc[0]=9192, vc[1]=9202. Hardcoding these here so the test asserts
    // against the same addresses the proxy listens on.
    private static final String VC_KEEP_BOOTSTRAP = "localhost:9192";
    private static final String VC_REMOVE_BOOTSTRAP = "localhost:9202";

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration PRODUCE_CONSUME_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration REJECTION_TIMEOUT = Duration.ofSeconds(8);

    @Test
    void shouldStopRemovedVcButContinueServingOthersEndToEnd(@BrokerCluster KafkaCluster cluster) throws Exception {
        String topic = "hotreload-remove-it-" + System.currentTimeMillis();
        createTopicOnUpstream(cluster, topic);

        Configuration twoVcConfig = proxy(cluster.getBootstrapServers(), VC_KEEP_NAME, VC_REMOVE_NAME).build();
        Configuration oneVcConfig = proxy(cluster.getBootstrapServers(), VC_KEEP_NAME).build();

        try (KafkaProxy proxy = new KafkaProxy(new ServiceBasedPluginFactoryRegistry(), twoVcConfig, Features.defaultFeatures())) {
            proxy.startup();

            // Phase 1: both VCs serve produce + consume.
            LOGGER.info("Phase 1: producing + consuming through both VCs");
            assertProduceConsumeRoundTrip(VC_KEEP_BOOTSTRAP, topic, "phase1-keep");
            assertProduceConsumeRoundTrip(VC_REMOVE_BOOTSTRAP, topic, "phase1-remove");

            // Phase 2: reconfigure removes vc-remove. The orchestrator's pure-remove path
            // drains vc-remove (no live connections at this point since we closed them in
            // phase 1) and transitions it to STOPPED.
            LOGGER.info("Phase 2: reconfiguring to remove '{}'", VC_REMOVE_NAME);
            var result = proxy.reconfigure(oneVcConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean pure-remove")
                    .isFalse();

            // Phase 3: vc-keep continues to serve traffic — verifies unaffected VCs are
            // genuinely undisturbed by the reconfigure.
            LOGGER.info("Phase 3: verifying '{}' still serves produce + consume", VC_KEEP_NAME);
            assertProduceConsumeRoundTrip(VC_KEEP_BOOTSTRAP, topic, "phase3-keep");

            // Phase 4: new connections to vc-remove are rejected. The acceptor channel is
            // still bound (a documented limitation until step 3 of the staircase introduces
            // proper port unbinding), but the SERVING-state guard closes connections at the
            // Kafka protocol layer, so the producer's send() future fails.
            LOGGER.info("Phase 4: verifying '{}' no longer accepts traffic", VC_REMOVE_NAME);
            assertProducerFailsAgainstRemovedVc(VC_REMOVE_BOOTSTRAP, topic);
        }
    }

    /**
     * Produce a random batch of records (50-100) through the proxy at {@code proxyBootstrap},
     * then consume them back through the same proxy. Records are keyed {@code marker-0},
     * {@code marker-1}, ... so the consumer can isolate this round-trip's records from
     * earlier phases' records that share the same topic.
     *
     * <p>Asserts that the consumer observes exactly {@code N} unique keys carrying this
     * call's {@code marker} — proving the full proxy I/O round-trip is working *and* that
     * no records are dropped mid-stream.
     */
    private static void assertProduceConsumeRoundTrip(String proxyBootstrap, String topic, String marker) throws Exception {
        int messageCount = ThreadLocalRandom.current().nextInt(50, 101);
        LOGGER.info("Producing {} records via {} (marker='{}')", messageCount, proxyBootstrap, marker);

        var sendFutures = new ArrayList<Future<RecordMetadata>>(messageCount);
        try (Producer<String, String> producer = newProducer(proxyBootstrap, false)) {
            for (int i = 0; i < messageCount; i++) {
                sendFutures.add(producer.send(new ProducerRecord<>(topic, marker + "-" + i, marker + "-value-" + i)));
            }
            // Wait for each ack — fails fast if any individual send didn't make it through
            // the proxy + broker round-trip within the timeout budget.
            for (Future<RecordMetadata> f : sendFutures) {
                f.get(PRODUCE_CONSUME_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            }
        }

        try (Consumer<String, String> consumer = newConsumer(proxyBootstrap, "hotreload-it-consumer-" + marker)) {
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
                    .as("consumer should have read all %d records (marker='%s') via proxy %s within %s",
                            messageCount, marker, proxyBootstrap, PRODUCE_CONSUME_TIMEOUT)
                    .hasSize(messageCount);
        }
    }

    /**
     * Open a fresh producer against the removed VC and verify it cannot deliver a record.
     * The exact exception depends on how the Kafka client interprets the connection rejection
     * (NetworkException, TimeoutException, etc.); we accept any failure from {@code send().get()}.
     */
    private static void assertProducerFailsAgainstRemovedVc(String proxyBootstrap, String topic) {
        try (Producer<String, String> producer = newProducer(proxyBootstrap, true)) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(topic, "should-fail", "should-fail-value"))
                    .get(REJECTION_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .as("producer should fail to deliver to the removed VC at %s", proxyBootstrap)
                    .isInstanceOf(ExecutionException.class);
        }
    }

    private static void createTopicOnUpstream(KafkaCluster cluster, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", cluster.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * @param shortTimeouts when {@code true}, configures aggressively short timeouts so the
     *                      "expected to fail" path completes within {@link #REJECTION_TIMEOUT}
     *                      rather than the default Kafka client delivery-timeout (2 minutes).
     */
    private static Producer<String, String> newProducer(String bootstrap, boolean shortTimeouts) {
        var cfg = new HashMap<String, Object>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        if (shortTimeouts) {
            cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
            cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5_000);
            cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
            cfg.put(ProducerConfig.RETRIES_CONFIG, 1);
            cfg.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 100);
            cfg.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
        }
        return new KafkaProducer<>(cfg);
    }

    private static Consumer<String, String> newConsumer(String bootstrap, String groupId) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }
}
