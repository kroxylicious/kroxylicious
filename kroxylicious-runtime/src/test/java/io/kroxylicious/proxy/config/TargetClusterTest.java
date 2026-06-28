/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

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

    @MethodSource()
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
}