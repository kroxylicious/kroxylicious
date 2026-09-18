/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.config.Features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Opt-in test against the real Confluent MDS and broker started by integration/compose.yaml. */
@EnabledIfSystemProperty(named = "mds.integration", matches = "true")
class MdsConfluentIT {
    private final Path generated = Path.of(System.getProperty("mds.integration.dir", "integration/generated")).toAbsolutePath();

    @Test
    void mdsRejectsImpersonationOfProtectedUser() {
        // Given
        String protectedUser = "ANONYMOUS";

        // When
        // Then
        assertThatThrownBy(() -> ConfluentTestSupport.token(generated, protectedUser))
                .hasRootCauseMessage("MDS authentication failed: MDS_HTTP (code=403)");
    }

    @Test
    void enforcesTheCertificateUsersRbacPermissions() throws Exception {
        // Given
        var parser = new ConfigParser();
        var config = parser.parseConfiguration(Files.readString(generated.resolve("proxy.yaml")));
        String value = UUID.randomUUID().toString();
        try (var proxy = new KafkaProxy(parser, config, Features.defaultFeatures())) {
            var shutdown = proxy.startup();
            var aliceProperties = ConfluentTestSupport.clientProperties(generated, "alice");
            aliceProperties.put("enable.idempotence", false);
            aliceProperties.put("max.block.ms", 15000);
            aliceProperties.put("delivery.timeout.ms", 15000);
            aliceProperties.put("request.timeout.ms", 5000);
            var bobProperties = new HashMap<>(aliceProperties);
            bobProperties.putAll(ConfluentTestSupport.clientProperties(generated, "bob"));
            var consumerProperties = ConfluentTestSupport.clientProperties(generated, "alice");
            consumerProperties.put("group.id", "mds-test");
            consumerProperties.put("auto.offset.reset", "earliest");
            consumerProperties.put("enable.auto.commit", false);
            try (var alice = new KafkaProducer<>(aliceProperties, new StringSerializer(), new StringSerializer());
                    var bob = new KafkaProducer<>(bobProperties, new StringSerializer(), new StringSerializer());
                    var consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
                // When
                var sent = alice.send(new ProducerRecord<>("mds-allowed", value)).get(20, TimeUnit.SECONDS);
                var denied = bob.send(new ProducerRecord<>("mds-allowed", value));
                consumer.subscribe(List.of("mds-allowed"));
                boolean received = false;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (!received && System.nanoTime() < deadline) {
                    for (var consumedRecord : consumer.poll(Duration.ofMillis(500))) {
                        received |= value.equals(consumedRecord.value());
                    }
                }

                // Then
                assertThat(shutdown).isNotDone();
                assertThat(sent.topic()).isEqualTo("mds-allowed");
                assertThat(received).isTrue();
                assertThatThrownBy(() -> denied.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(TopicAuthorizationException.class);
            }
        }
    }

}
