/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.ParameterizedInvocationConstants.ARGUMENT_SET_NAME_OR_ARGUMENTS_WITH_NAMES_PLACEHOLDER;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RoundRobinBootstrapSelectionStrategyTest {

    private final RoundRobinBootstrapSelectionStrategy strategy;

    RoundRobinBootstrapSelectionStrategyTest() {
        this.strategy = new RoundRobinBootstrapSelectionStrategy();
    }

    private static Stream<Arguments> provideArguments() {
        final var bootstrapServers = List.of(
                new HostPort("host0", 9092),
                new HostPort("host1", 9093),
                new HostPort("host2", 9094));
        return Stream.of(
                Arguments.argumentSet("select first", bootstrapServers, bootstrapServers.get(0)),
                Arguments.argumentSet("select second", bootstrapServers, bootstrapServers.get(1)),
                Arguments.argumentSet("select third", bootstrapServers, bootstrapServers.get(2)),
                Arguments.argumentSet("round over and select first", bootstrapServers, bootstrapServers.get(0)));
    }

    @ParameterizedTest(name = ARGUMENT_SET_NAME_OR_ARGUMENTS_WITH_NAMES_PLACEHOLDER)
    @MethodSource("provideArguments")
    void shouldReturnAServerFromTheListInRoundRobinFashion(List<HostPort> servers, HostPort expectedServer) {
        assertThat(strategy.apply(servers)).isEqualTo(expectedServer);
    }

}