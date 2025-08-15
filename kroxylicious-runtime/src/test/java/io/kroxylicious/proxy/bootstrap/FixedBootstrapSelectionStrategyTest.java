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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedBootstrapSelectionStrategyTest {

    @Test
    void shouldRejectNegativeValuesForChoice() {
        assertThatThrownBy(() -> new FixedBootstrapSelectionStrategy(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldSelectBootstrapServerBasedOnGivenChoice() {
        final var bootstrapServers = List.of(
                new HostPort("host0", 9092),
                new HostPort("host1", 9093),
                new HostPort("host2", 9094));
        final int choice = 1;
        final var strategy = new FixedBootstrapSelectionStrategy(choice);
        assertThat(strategy.apply(bootstrapServers)).isEqualTo(bootstrapServers.get(choice));
    }
}