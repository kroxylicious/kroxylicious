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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.config.Features;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in exercise of group coordination and reconnects across real MDS token expiry. */
@EnabledIfSystemProperty(named = "mds.integration", matches = "true")
class MdsConfluentRebalanceIT {
    @Test
    void deliversRecordsThroughRebalanceAndTokenExpiry() throws Exception {
        // Given
        var generated = Path.of(System.getProperty("mds.integration.dir", "integration/generated")).toAbsolutePath();
        var parser = new ConfigParser();
        var config = parser.parseConfiguration(Files.readString(generated.resolve("proxy.yaml")));
        var token = ConfluentTestSupport.token(generated);
        if (Duration.between(Instant.now(), token.expiresAt()).compareTo(Duration.ofSeconds(40)) > 0) {
            throw new IllegalStateException("Use the Compose environment's 30-second MDS tokens for this test");
        }
        var producerProperties = ConfluentTestSupport.clientProperties(generated, "alice");
        producerProperties.put("enable.idempotence", false);
        producerProperties.put("request.timeout.ms", 5000);
        producerProperties.put("delivery.timeout.ms", 15000);
        var consumerProperties = ConfluentTestSupport.clientProperties(generated, "alice");
        consumerProperties.put("group.id", "mds-test");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("enable.auto.commit", false);
        Set<String> sent = new HashSet<>();
        Set<String> received = new HashSet<>();
        String prefix = UUID.randomUUID() + ":";
        try (var proxy = new KafkaProxy(parser, config, Features.defaultFeatures())) {
            var shutdown = proxy.startup();
            try (var producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
                    var first = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());
                    var second = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
                first.subscribe(List.of("mds-allowed"));
                long assignmentDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (first.assignment().isEmpty() && System.nanoTime() < assignmentDeadline) {
                    first.poll(Duration.ofMillis(200));
                }
                int initialAssignment = first.assignment().size();

                // When
                second.subscribe(List.of("mds-allowed"));
                boolean rebalanced = false;
                Instant finish = token.expiresAt().plusSeconds(10);
                while (Instant.now().isBefore(finish)) {
                    for (int partition = 0; partition < 2; partition++) {
                        String value = prefix + sent.size();
                        producer.send(new ProducerRecord<>("mds-allowed", partition, null, value)).get(20, TimeUnit.SECONDS);
                        sent.add(value);
                    }
                    collect(first, prefix, received);
                    collect(second, prefix, received);
                    rebalanced |= first.assignment().size() == 1 && second.assignment().size() == 1;
                }
                long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!received.containsAll(sent) && System.nanoTime() < drainDeadline) {
                    collect(first, prefix, received);
                    collect(second, prefix, received);
                }

                // Then
                assertThat(shutdown).isNotDone();
                assertThat(initialAssignment).isEqualTo(2);
                assertThat(rebalanced).isTrue();
                assertThat(sent).isNotEmpty();
                assertThat(received).containsAll(sent);
                assertThat(Instant.now()).isAfter(token.expiresAt());
            }
        }
    }

    private static void collect(KafkaConsumer<String, String> consumer, String prefix, Set<String> received) {
        for (var consumedRecord : consumer.poll(Duration.ofMillis(200))) {
            if (consumedRecord.value().startsWith(prefix)) {
                received.add(consumedRecord.value());
            }
        }
    }
}
