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
    void shouldReturnARandomServerChosenFromTheList() {
        final var bootstrapServers = List.of(
                new HostPort("host0", 9092),
                new HostPort("host1", 9093),
                new HostPort("host2", 9094));
        final var strategy = new RandomBootstrapSelectionStrategy();
        assertThat(strategy.apply(bootstrapServers)).isIn(bootstrapServers);
    }

}