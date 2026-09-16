/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

class RandomBootstrapSelectionStrategyTest {

    @Test
    void shouldReturnStrategy() {
        // Given
        var strategy = new RandomBootstrapSelectionStrategy();

        // When/Then
        assertThat(strategy.getStrategy()).isEqualTo("random");
    }

    @Test
    void shouldReturnARandomServerChosenFromTheList() {
        final var bootstrapServers = List.of(
                new HostPort("host0", 9092),
                new HostPort("host1", 9093),
                new HostPort("host2", 9094));
        final var strategy = new RandomBootstrapSelectionStrategy();
        assertThat(strategy.apply(bootstrapServers)).isIn(bootstrapServers);
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
    void shouldBeEqualToAnotherInstanceRegardlessOfRandomState() {
        // Given
        var strategy1 = new RandomBootstrapSelectionStrategy();
        var strategy2 = new RandomBootstrapSelectionStrategy();
        var servers = List.of(
                new HostPort("host0", 9092),
                new HostPort("host1", 9093),
                new HostPort("host2", 9094));
        strategy1.apply(servers);

        // When
        strategy1.apply(servers);

        // Then
        assertThat(strategy1).isEqualTo(strategy2);
        assertThat(strategy1.hashCode()).isEqualTo(strategy2.hashCode());
    }

    @Test
    void shouldHaveConsistentHashCode() {
        // Given
        var strategy = new RandomBootstrapSelectionStrategy();
        var servers = List.of(new HostPort("host1", 9092));
        int hash1 = strategy.hashCode();

        // When
        strategy.apply(servers);

        // Then
        int hash2 = strategy.hashCode();
        assertThat(hash1).isEqualTo(hash2);
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