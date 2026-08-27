/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity.kafka;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire-fidelity parity between Apache Kafka's {@link ApiKeys} and the Kroxylicious-owned
 * {@link io.kroxylicious.kafka.common.protocol.ApiKeys}. The generated enum and the kafka-clients
 * jar are both pinned to {@code kafka.message-spec.version} (see this module's pom), so there is no
 * version skew: every api key must agree on id, name, supported version range and per-version
 * header versions. A divergence here means the proxy would advertise or negotiate a different
 * protocol surface than a real broker.
 */
class ApiKeysFidelityTest {

    @Test
    void sameApiKeyConstants() {
        // Given
        var kafkaNames = Arrays.stream(ApiKeys.values()).map(Enum::name).collect(Collectors.toSet());
        var kroxyliciousNames = Arrays.stream(io.kroxylicious.kafka.common.protocol.ApiKeys.values())
                .map(Enum::name).collect(Collectors.toSet());

        // When / Then
        assertThat(kroxyliciousNames).containsExactlyInAnyOrderElementsOf(kafkaNames);
    }

    @Test
    void sameApiKeyOrder() {
        // Given
        var kafkaNames = Arrays.stream(ApiKeys.values()).map(Enum::name).toList();
        var kroxyliciousNames = Arrays.stream(io.kroxylicious.kafka.common.protocol.ApiKeys.values())
                .map(Enum::name).toList();

        // When / Then
        assertThat(kroxyliciousNames).containsExactlyElementsOf(kafkaNames);
    }

    @Test
    void produceApiVersionsResponseMinVersionMatches() {
        // When / Then
        assertThat(io.kroxylicious.kafka.common.protocol.ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION)
                .isEqualTo(ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void apiKeyMatchesKafka(ApiKeys kafka) {
        // Given
        var kroxylicious = io.kroxylicious.kafka.common.protocol.ApiKeys.valueOf(kafka.name());

        // When / Then
        assertThat(kroxylicious.id).isEqualTo(kafka.id);
        assertThat(kroxylicious.name).isEqualTo(kafka.name);
        assertThat(kroxylicious.oldestVersion()).isEqualTo(kafka.oldestVersion());
        assertThat(kroxylicious.latestVersion()).isEqualTo(kafka.latestVersion());
        assertThat(kroxylicious.latestVersion(false)).isEqualTo(kafka.latestVersion(false));
        assertThat(kroxylicious.latestVersion(true)).isEqualTo(kafka.latestVersion(true));
        assertThat(kroxylicious.hasValidVersion()).isEqualTo(kafka.hasValidVersion());
        assertThat(kroxylicious.allVersions()).isEqualTo(kafka.allVersions());
        assertThat(kroxylicious.messageType.apiKey()).isEqualTo(kroxylicious.id);
        assertThat(kroxylicious.messageType.name).isEqualTo(kafka.name);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void isVersionSupportedMatchesKafka(ApiKeys kafka) {
        // Given
        var kroxylicious = io.kroxylicious.kafka.common.protocol.ApiKeys.valueOf(kafka.name());
        short belowRange = (short) (kafka.oldestVersion() - 1);
        short aboveRange = (short) (kafka.latestVersion() + 1);

        // When / Then
        for (short version : kafka.allVersions()) {
            assertThat(kroxylicious.isVersionSupported(version))
                    .as("isVersionSupported(%d) for %s", version, kafka.name())
                    .isTrue();
        }
        assertThat(kroxylicious.isVersionSupported(belowRange))
                .as("isVersionSupported(%d) for %s", belowRange, kafka.name())
                .isFalse();
        assertThat(kroxylicious.isVersionSupported(aboveRange))
                .as("isVersionSupported(%d) for %s", aboveRange, kafka.name())
                .isFalse();
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void lookupByIdMatchesKafka(ApiKeys kafka) {
        // When / Then
        assertThat(io.kroxylicious.kafka.common.protocol.ApiKeys.hasId(kafka.id)).isTrue();
        assertThat(io.kroxylicious.kafka.common.protocol.ApiKeys.forId(kafka.id).name()).isEqualTo(kafka.name());
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void headerVersionsMatchKafka(ApiKeys kafka) {
        // Given
        var kroxylicious = io.kroxylicious.kafka.common.protocol.ApiKeys.valueOf(kafka.name());

        // When / Then
        for (short version : kafka.allVersions()) {
            assertThat(kroxylicious.requestHeaderVersion(version))
                    .as("requestHeaderVersion(%d) for %s", version, kafka.name())
                    .isEqualTo(kafka.requestHeaderVersion(version));
            assertThat(kroxylicious.responseHeaderVersion(version))
                    .as("responseHeaderVersion(%d) for %s", version, kafka.name())
                    .isEqualTo(kafka.responseHeaderVersion(version));
        }
    }
}
