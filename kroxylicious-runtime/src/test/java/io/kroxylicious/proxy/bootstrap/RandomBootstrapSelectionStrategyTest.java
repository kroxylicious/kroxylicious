/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

class RandomBootstrapSelectionStrategyTest {

    private static final List<HostPort> SERVERS = List.of(
            new HostPort("host0", 9092),
            new HostPort("host1", 9093),
            new HostPort("host2", 9094));

    @Test
    void shouldReturnStrategy() {
        // Given
        var strategy = new RandomBootstrapSelectionStrategy();

        // When/Then
        assertThat(strategy.getStrategy()).isEqualTo("random");
    }

    @Test
    void shouldReturnARandomServerChosenFromTheList() {
        // Given
        final var selector = new RandomBootstrapSelectionStrategy().newSelector();

        // When
        var selected = selector.select(SERVERS);

        // Then
        assertThat(selected).isIn(SERVERS);
    }

    @Test
    void selectorShouldBeSafeForConcurrentUse() throws Exception {
        // Given
        var sharedSelector = new RandomBootstrapSelectionStrategy().newSelector();
        int threads = 6;
        int selectionsPerThread = 10_000;
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
            assertThat(selections)
                    .hasSize(threads * selectionsPerThread)
                    .allSatisfy(selected -> assertThat(selected).isIn(SERVERS));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldImplementEquals() {
        // Given
        var strategy = new RandomBootstrapSelectionStrategy();
        var same = new RandomBootstrapSelectionStrategy();
        var different = new RoundRobinBootstrapSelectionStrategy();

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
        var strategy1 = new RandomBootstrapSelectionStrategy();
        var strategy2 = new RandomBootstrapSelectionStrategy();
        var selector1 = strategy1.newSelector();

        // When
        selector1.select(SERVERS);

        // Then
        assertThat(strategy1).isEqualTo(strategy2);
        assertThat(strategy1.hashCode()).isEqualTo(strategy2.hashCode());
    }

    @Test
    void shouldNotBeEqualToNull() {
        // Given
        var strategy = new RandomBootstrapSelectionStrategy();

        // Then
        assertThat(strategy).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentStrategyType() {
        // Given
        var random = new RandomBootstrapSelectionStrategy();
        var roundRobin = new RoundRobinBootstrapSelectionStrategy();

        // Then
        assertThat(random).isNotEqualTo(roundRobin);
    }

}
