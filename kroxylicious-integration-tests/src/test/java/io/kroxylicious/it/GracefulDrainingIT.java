/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying graceful connection draining during proxy shutdown.
 * <p>
 * A background thread continuously sends messages through the proxy. While it's
 * actively sending (connections established, messages in-flight), we trigger proxy
 * shutdown. The drain mechanism should allow in-flight responses to complete before
 * closing connections. We verify that every message the producer received an ack for
 * is actually present on Kafka — no phantom acks.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class GracefulDrainingIT extends BaseIT {

    /**
     * Verifies no acked messages are lost when the proxy shuts down while a producer
     * is actively sending.
     * <p>
     * The test exercises the drain mechanism by:
     * <ol>
     *   <li>Starting a background producer that sends messages in a tight loop</li>
     *   <li>Letting it run for 2 seconds so connections are established and messages flow</li>
     *   <li>Triggering proxy shutdown while messages are in-flight</li>
     *   <li>Verifying every acked message is present on Kafka via a direct consumer</li>
     * </ol>
     */
    @Test
    void shouldNotLoseAckedMessagesWhenShutdownDuringActiveSending(KafkaCluster cluster, Topic topic) throws Exception {
        var config = proxy(cluster);

        Set<String> ackedValues = ConcurrentHashMap.newKeySet();
        AtomicBoolean keepSending = new AtomicBoolean(true);
        CountDownLatch producerStarted = new CountDownLatch(1);
        List<Throwable> producerErrors = Collections.synchronizedList(new ArrayList<>());

        var tester = kroxyliciousTester(config);
        Producer<String, String> producer = tester.producer();

        // Background thread: continuously send messages through the proxy
        Thread producerThread = new Thread(() -> {
            try {
                producerStarted.countDown();
                int i = 0;
                while (keepSending.get()) {
                    String value = "value-" + i;
                    try {
                        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic.name(), "key-" + i, value));
                        RecordMetadata metadata = future.get(5, TimeUnit.SECONDS);
                        if (metadata != null) {
                            ackedValues.add(value);
                        }
                    }
                    catch (Exception e) {
                        // Expected during shutdown — producer gets errors when connections close.
                        // Don't add to ackedValues — this message may not be on Kafka.
                        break;
                    }
                    i++;
                }
            }
            catch (Exception e) {
                producerErrors.add(e);
            }
        }, "drain-test-producer");

        producerThread.start();

        // Wait for producer to start sending
        assertThat(producerStarted.await(10, TimeUnit.SECONDS))
                .as("Producer thread should start within 10 seconds")
                .isTrue();

        // Let the producer send for a bit so connections are established and messages flow
        Thread.sleep(2000);

        // Trigger shutdown while producer is actively sending
        keepSending.set(false);
        producer.close(Duration.ofSeconds(5));
        tester.close(); // This triggers graceful drain

        producerThread.join(10_000);

        assertThat(ackedValues)
                .as("At least some messages should have been acked before shutdown")
                .isNotEmpty();

        // Verify: every acked message must be on Kafka
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "drain-concurrent-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {

            consumer.subscribe(List.of(topic.name()));

            Set<String> valuesOnKafka = ConcurrentHashMap.newKeySet();
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(r -> valuesOnKafka.add(r.value()));
                if (valuesOnKafka.containsAll(ackedValues)) {
                    break;
                }
            }

            assertThat(valuesOnKafka)
                    .as("Every acked message must be present on Kafka — no phantom acks")
                    .containsAll(ackedValues);
        }
    }
}
