/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NettySettingsTest {

    private static final Optional<Duration> NEGATIVE = Optional.of(Duration.ofSeconds(-1));
    private static final Optional<Duration> EMPTY = Optional.empty();

    @Test
    void shouldRejectNegativeShutdownQuietPeriod() {
        assertThatThrownBy(() -> new NettySettings(EMPTY, NEGATIVE, EMPTY, EMPTY, EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownQuietPeriod");
    }

    @Test
    void shouldRejectNegativeShutdownTimeout() {
        assertThatThrownBy(() -> new NettySettings(EMPTY, EMPTY, NEGATIVE, EMPTY, EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
    }

    @Test
    void shouldRejectNegativeAuthenticatedIdleTimeout() {
        assertThatThrownBy(() -> new NettySettings(EMPTY, EMPTY, EMPTY, NEGATIVE, EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authenticatedIdleTimeout");
    }

    @Test
    void shouldRejectNegativeUnauthenticatedIdleTimeout() {
        assertThatThrownBy(() -> new NettySettings(EMPTY, EMPTY, EMPTY, EMPTY, NEGATIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unauthenticatedIdleTimeout");
    }
}
