/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.config.Features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Ordinary SSL clients must keep their connections through several real MDS token lifetimes. */
@EnabledIfSystemProperty(named = "mds.integration", matches = "true")
class MdsConfluentReauthenticationIT {
    @Test
    void keepsConnectionsAcrossActiveRenewalsAndAnIdleProducerSession() throws Exception {
        // Given
        var generated = Path.of(System.getProperty("mds.integration.dir", "integration/generated")).toAbsolutePath();
        var parser = new ConfigParser();
        var config = parser.parseConfiguration(Files.readString(generated.resolve("proxy.yaml")));
        var lifetime = Duration.between(Instant.now(), ConfluentTestSupport.token(generated).expiresAt());
        if (lifetime.isNegative() || lifetime.compareTo(Duration.ofSeconds(40)) > 0) {
            throw new IllegalStateException("This test requires the Compose environment's 30-second MDS tokens");
        }
        var producerProperties = ConfluentTestSupport.clientProperties(generated, "alice");
        producerProperties.put("enable.idempotence", false);
        producerProperties.put("enable.metrics.push", false);
        producerProperties.put("request.timeout.ms", 5000);
        producerProperties.put("delivery.timeout.ms", 15000);
        var consumerProperties = ConfluentTestSupport.clientProperties(generated, "alice");
        consumerProperties.put("group.id", "mds-test");
        consumerProperties.put("auto.offset.reset", "latest");
        consumerProperties.put("enable.auto.commit", false);
        consumerProperties.put("enable.metrics.push", false);
        try (var proxy = new KafkaProxy(parser, config, Features.defaultFeatures())) {
            var shutdown = proxy.startup();
            try (var producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
                    var consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of("mds-allowed"));
                long assignmentDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (consumer.assignment().size() != 2 && System.nanoTime() < assignmentDeadline) {
                    consumer.poll(Duration.ofMillis(200));
                }
                if (consumer.assignment().size() != 2) {
                    throw new IllegalStateException("The test consumer did not receive both partitions");
                }
                consumer.seekToEnd(consumer.assignment());
                for (var partition : consumer.assignment()) {
                    consumer.position(partition, Duration.ofSeconds(10));
                }
                exchange(producer, consumer);
                var before = connections(producer, consumer);

                // When
                long activeDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(75);
                while (System.nanoTime() < activeDeadline) {
                    exchange(producer, consumer);
                }
                var active = connections(producer, consumer);
                var denied = producer.send(new ProducerRecord<>("mds-forbidden", "unauthorized"));
                var denial = catchThrowable(() -> denied.get(20, TimeUnit.SECONDS));
                long idleDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
                while (System.nanoTime() < idleDeadline) {
                    consumer.poll(Duration.ofMillis(200));
                }
                exchange(producer, consumer);
                var afterIdle = connections(producer, consumer);

                // Then
                assertThat(shutdown).isNotDone();
                assertThat(active).isEqualTo(before);
                assertThat(afterIdle).isEqualTo(before);
                assertThat(denial).hasCauseInstanceOf(TopicAuthorizationException.class);
            }
        }
    }

    private static void exchange(KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) throws Exception {
        var keys = new HashSet<String>();
        for (int partition = 0; partition < 2; partition++) {
            String key = UUID.randomUUID().toString();
            keys.add(key);
            producer.send(new ProducerRecord<>("mds-allowed", partition, key, key)).get(20, TimeUnit.SECONDS);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!keys.isEmpty() && System.nanoTime() < deadline) {
            for (var consumedRecord : consumer.poll(Duration.ofMillis(200))) {
                if (keys.contains(consumedRecord.key()) && consumedRecord.key().equals(consumedRecord.value())) {
                    keys.remove(consumedRecord.key());
                }
            }
        }
        if (!keys.isEmpty()) {
            throw new IllegalStateException("Records missing or corrupted across reauthentication");
        }
        consumer.commitSync(Duration.ofSeconds(10));
    }

    private static List<Double> connections(KafkaProducer<?, ?> producer, KafkaConsumer<?, ?> consumer) {
        return List.of(metric(producer.metrics(), "connection-creation-total"), metric(producer.metrics(), "connection-close-total"),
                metric(consumer.metrics(), "connection-creation-total"), metric(consumer.metrics(), "connection-close-total"));
    }

    private static double metric(Map<MetricName, ? extends Metric> metrics, String name) {
        return metrics.entrySet().stream().filter(entry -> entry.getKey().name().equals(name))
                .mapToDouble(entry -> ((Number) entry.getValue().metricValue()).doubleValue()).findFirst().orElseThrow();
    }
}
