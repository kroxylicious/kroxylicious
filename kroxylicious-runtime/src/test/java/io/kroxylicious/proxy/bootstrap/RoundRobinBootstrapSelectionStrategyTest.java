/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.service.HostPort;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.ParameterizedInvocationConstants.ARGUMENT_SET_NAME_OR_ARGUMENTS_WITH_NAMES_PLACEHOLDER;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RoundRobinBootstrapSelectionStrategyTest {

    private static final List<HostPort> SERVERS = List.of(
            new HostPort("host0", 9092),
            new HostPort("host1", 9093),
            new HostPort("host2", 9094));

    private final BootstrapServerSelector selector;

    RoundRobinBootstrapSelectionStrategyTest() {
        this.selector = new RoundRobinBootstrapSelectionStrategy().newSelector();
    }

    private static Stream<Arguments> provideArguments() {
        return Stream.of(
                Arguments.argumentSet("select first", SERVERS, SERVERS.get(0)),
                Arguments.argumentSet("select second", SERVERS, SERVERS.get(1)),
                Arguments.argumentSet("select third", SERVERS, SERVERS.get(2)),
                Arguments.argumentSet("round over and select first", SERVERS, SERVERS.get(0)));
    }

    @ParameterizedTest(name = ARGUMENT_SET_NAME_OR_ARGUMENTS_WITH_NAMES_PLACEHOLDER)
    @MethodSource("provideArguments")
    void shouldReturnAServerFromTheListInRoundRobinFashion(List<HostPort> servers, HostPort expectedServer) {
        assertThat(selector.select(servers)).isEqualTo(expectedServer);
    }

    @Test
    void shouldReturnStrategy() {
        // Given
        var strategy = new RoundRobinBootstrapSelectionStrategy();

        // When/Then
        assertThat(strategy.getStrategy()).isEqualTo("round-robin");
    }

    @Test
    void newSelectorShouldReturnSelectorsWithIndependentState() {
        // Given
        var strategy = new RoundRobinBootstrapSelectionStrategy();
        var first = strategy.newSelector();
        var second = strategy.newSelector();
        first.select(SERVERS);
        first.select(SERVERS);

        // When
        var selected = second.select(SERVERS);

        // Then
        assertThat(selected).isEqualTo(SERVERS.get(0));
    }

    @Test
    void selectorShouldBeSafeForConcurrentUse() throws Exception {
        // Given
        var sharedSelector = new RoundRobinBootstrapSelectionStrategy().newSelector();
        int threads = 6;
        int selectionsPerThread = 9_999; // total is a multiple of SERVERS.size(), so each server is chosen equally often
        var start = new CountDownLatch(1);
        var tasks = new ArrayList<Callable<List<HostPort>>>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                start.await();
                var selections = new ArrayList<HostPort>(selectionsPerThread);
                for (int j = 0; j < selectionsPerThread; j++) {
                    selections.add(sharedSelector.select(SERVERS));
                }
                return selections;
            });
        }
        var executor = Executors.newFixedThreadPool(threads);
        try {
            // When
            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();
            var selections = new ArrayList<HostPort>();
            for (Future<List<HostPort>> future : futures) {
                selections.addAll(future.get());
            }

            // Then
            long expectedPerServer = (long) threads * selectionsPerThread / SERVERS.size();
            assertThat(selections.stream().collect(groupingBy(s -> s, counting())))
                    .containsOnlyKeys(SERVERS)
                    .allSatisfy((server, count) -> assertThat(count).isEqualTo(expectedPerServer));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldImplementEquals() {
        // Given
        var strategy = new RoundRobinBootstrapSelectionStrategy();
        var same = new RoundRobinBootstrapSelectionStrategy();
        var different = new RandomBootstrapSelectionStrategy();

        // When/Then
        // noinspection EqualsWithItself
        assertThat(strategy.equals(strategy)).isTrue();
        assertThat(strategy.equals(same)).isTrue();
        // noinspection EqualsBetweenInconvertibleTypes
        assertThat(strategy.equals(different)).isFalse();
    }

    @Test
    void shouldBeEqualToAnotherInstanceRegardlessOfSelectorState() {
        // Given
        var strategy1 = new RoundRobinBootstrapSelectionStrategy();
        var strategy2 = new RoundRobinBootstrapSelectionStrategy();
        var selector1 = strategy1.newSelector();

        // When
        selector1.select(SERVERS);
        selector1.select(SERVERS);

        // Then
        assertThat(strategy1).isEqualTo(strategy2);
        assertThat(strategy1.hashCode()).isEqualTo(strategy2.hashCode());
    }

    @Test
    void shouldNotBeEqualToNull() {
        // Given
        var strategy = new RoundRobinBootstrapSelectionStrategy();

        // Then
        assertThat(strategy).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentStrategyType() {
        // Given
        var roundRobin = new RoundRobinBootstrapSelectionStrategy();
        var random = new RandomBootstrapSelectionStrategy();

        // Then
        assertThat(roundRobin).isNotEqualTo(random);
    }

    @Test
    void shouldUseTheSameHashCodeForAllInstances() {
        // Given
        var strategy1 = new RoundRobinBootstrapSelectionStrategy();
        var strategy2 = new RoundRobinBootstrapSelectionStrategy();

        // Then
        assertThat(Map.of(strategy1, "a")).containsKey(strategy2);
    }

}
