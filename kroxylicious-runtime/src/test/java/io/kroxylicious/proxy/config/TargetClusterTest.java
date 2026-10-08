/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TargetClusterTest {

    @Test
    void shouldRejectEmptyBootstrapServers() {
        Optional<Tls> empty = Optional.empty();
        assertThatThrownBy(() -> new TargetCluster(null, empty))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @MethodSource
    @ParameterizedTest
    void parseBootstrapServers(String bootstrapServers, List<HostPort> expected) {
        // given
        TargetCluster targetCluster = new TargetCluster(bootstrapServers, Optional.empty());
        // when
        List<HostPort> actual = targetCluster.bootstrapServersList();
        // then
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    static Stream<Arguments> parseBootstrapServers() {
        return Stream.of(
                Arguments.argumentSet("space between entries",
                        "192.168.0.1:9092, 192.168.0.2:9092, 192.168.0.3:9092",
                        List.of(HostPort.parse("192.168.0.1:9092"), HostPort.parse("192.168.0.2:9092"), HostPort.parse("192.168.0.3:9092"))),
                Arguments.argumentSet("single entry",
                        "localhost:9092",
                        List.of(HostPort.parse("localhost:9092"))),
                Arguments.argumentSet("multiple entries, no whitespace",
                        "localhost:9092,localhost:9093",
                        List.of(HostPort.parse("localhost:9092"), HostPort.parse("localhost:9093"))),
                Arguments.argumentSet("preceding whitepace",
                        "  10.0.0.1:9092 ,  10.0.0.2:9092",
                        List.of(HostPort.parse("10.0.0.1:9092"), HostPort.parse("10.0.0.2:9092"))),
                Arguments.argumentSet("trailing whitepace",
                        "10.0.0.1:9092 ,  10.0.0.2:9092  ",
                        List.of(HostPort.parse("10.0.0.1:9092"), HostPort.parse("10.0.0.2:9092"))));
    }

    @Test
    void shouldApplyDefaultConnectTimeoutWhenUnset() {
        // Given
        var targetCluster = new TargetCluster("broker:9092", Optional.empty());

        // When
        var effective = targetCluster.effectiveConnectTimeout();

        // Then
        assertThat(targetCluster.connectTimeout()).isNull();
        assertThat(effective).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void shouldReturnExplicitConnectTimeout() {
        // Given
        var targetCluster = new TargetCluster("broker:9092", Optional.empty(), null, Duration.ofSeconds(5));

        // When
        var effective = targetCluster.effectiveConnectTimeout();

        // Then
        assertThat(targetCluster.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(effective).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void shouldRejectZeroConnectTimeout() {
        // Given
        Optional<Tls> empty = Optional.empty();

        // When / Then
        assertThatThrownBy(() -> new TargetCluster("broker:9092", empty, null, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectTimeout");
    }

    @Test
    void shouldRejectNegativeConnectTimeout() {
        // Given
        Optional<Tls> empty = Optional.empty();
        var negative = Duration.ofSeconds(-1);

        // When / Then
        assertThatThrownBy(() -> new TargetCluster("broker:9092", empty, null, negative))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectTimeout");
    }

    @Test
    void shouldRejectConnectTimeoutExceedingIntegerMaxMillis() {
        // Given
        Optional<Tls> empty = Optional.empty();
        var tooLong = Duration.ofMillis(Integer.MAX_VALUE).plusMillis(1);

        // When / Then
        assertThatThrownBy(() -> new TargetCluster("broker:9092", empty, null, tooLong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectTimeout");
    }

    @Test
    void shouldAcceptConnectTimeoutAtIntegerMaxMillis() {
        // Given
        var atLimit = Duration.ofMillis(Integer.MAX_VALUE);

        // When
        var targetCluster = new TargetCluster("broker:9092", Optional.empty(), null, atLimit);

        // Then
        assertThat(targetCluster.effectiveConnectTimeout()).isEqualTo(atLimit);
    }

    @Test
    void shouldRejectConnectTimeoutTooLargeToConvertToMillis() {
        // Given
        Optional<Tls> empty = Optional.empty();
        // toMillis() would overflow and throw ArithmeticException for this duration; validation must
        // reject it with IllegalArgumentException instead, via the non-overflowing compareTo bound.
        var overflowing = Duration.ofSeconds(9223372036854776L);

        // When / Then
        assertThatThrownBy(() -> new TargetCluster("broker:9092", empty, null, overflowing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectTimeout");
    }

    @Test
    void shouldForwardSelectionStrategyAndLeaveConnectTimeoutUnset() {
        // Given
        var viaCanonicalWithNull = new TargetCluster("broker:9092", Optional.empty(), null, null);

        // When
        var viaOverload = new TargetCluster("broker:9092", Optional.empty(), null);

        // Then
        assertThat(viaOverload).isEqualTo(viaCanonicalWithNull);
        assertThat(viaOverload.connectTimeout()).isNull();
    }
}