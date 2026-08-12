/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.github.sambarker.logsquelcher.CapturedLogs;
import io.github.sambarker.logsquelcher.LogSquelcherExtension;

import io.kroxylicious.filter.protocollogger.ProtocolLogger;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
@ExtendWith(LogSquelcherExtension.class)
class ProtocolLoggerFilterIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void trafficRoundTripsCorrectlyWithFilterInChain(@BrokerCluster KafkaCluster cluster, Topic topic) {
        // Given
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogger.class.getName())
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                    .succeedsWithin(TIMEOUT);
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k2", "v2")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            // Then
            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(2)
                    .extracting(ConsumerRecord::key, ConsumerRecord::value)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("k1", "v1"),
                            org.assertj.core.groups.Tuple.tuple("k2", "v2"));
        }
    }

    @Test
    void apiKeyGatingEmitsOnlyConfiguredKeys(@BrokerCluster KafkaCluster cluster, Topic topic, CapturedLogs capturedLogs) {
        // Given
        var loggerName = "it.apiKeyGating";
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogger.class.getName())
                .withConfig("apiKeyNames", List.of("METADATA"), "loggerName", loggerName)
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "gating-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "gated")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(1)
                    .first()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("gated");
        }

        // Then
        assertThat(eventsFor(capturedLogs, loggerName, Level.DEBUG))
                .isNotEmpty()
                .extracting(e -> extractApiKey(e.getMessage()))
                .containsOnly("METADATA");
    }

    // Log level gating cannot be tested at IT level because LogSquelcher (the
    // project-wide test SLF4J provider) enables all levels. The predicate-level
    // unit tests cover this with NOPLogger.

    @Test
    void realClientNegotiatedVersionsProduceValidEntries(@BrokerCluster KafkaCluster cluster, Topic topic, CapturedLogs capturedLogs) {
        // Given
        var loggerName = "it.realClientVersions";
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogger.class.getName())
                .withConfig("loggerName", loggerName)
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "real-version-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(1)
                    .first()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("v1");
        }

        // Then
        var events = eventsFor(capturedLogs, loggerName, Level.DEBUG);

        assertThat(events)
                .isNotEmpty()
                .extracting(e -> extractApiKey(e.getMessage()))
                .containsAnyOf("METADATA", "PRODUCE", "FETCH")
                .hasSizeGreaterThan(1);

        assertThat(events)
                .allSatisfy(event -> {
                    var entry = parseJson(event.getMessage());
                    var header = entry.get("header");
                    assertThat(header).isNotNull();
                    assertThat(header.get("apiKey").asText()).isNotEmpty();
                    assertThat(header.get("apiVersion").isInt()).isTrue();
                    assertThat(entry.has("payload")).isTrue();
                });

        assertThat(events)
                .filteredOn(e -> parseJson(e.getMessage()).path("header").path("type").asText().equals("RESPONSE"))
                .isNotEmpty()
                .allSatisfy(event -> {
                    var header = parseJson(event.getMessage()).get("header");
                    assertThat(header.get("apiKey").asText()).isNotEmpty();
                    assertThat(header.get("apiVersion").isInt()).isTrue();
                });
    }

    private static List<LoggingEvent> eventsFor(CapturedLogs capturedLogs, String loggerName, Level level) {
        return capturedLogs.logged().stream()
                .filter(e -> loggerName.equals(e.getLoggerName()))
                .filter(e -> level == e.getLevel())
                .toList();
    }

    private static String extractApiKey(String message) {
        return parseJson(message).path("header").path("apiKey").asText();
    }

    private static JsonNode parseJson(String message) {
        try {
            return MAPPER.readTree(message);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Log message is not valid JSON: " + message, e);
        }
    }
}
