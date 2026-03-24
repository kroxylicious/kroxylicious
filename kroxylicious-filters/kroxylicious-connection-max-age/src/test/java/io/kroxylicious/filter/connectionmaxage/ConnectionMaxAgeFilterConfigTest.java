/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionmaxage;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionMaxAgeFilterConfigTest {

    @Test
    void shouldAcceptValidMaxAgeWithoutJitter() {
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), null);
        assertThat(config.maxAge()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.jitter()).isNull();
    }

    @Test
    void shouldAcceptValidMaxAgeWithJitter() {
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), Duration.ofSeconds(30));
        assertThat(config.maxAge()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.jitter()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void shouldAcceptZeroJitter() {
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), Duration.ZERO);
        assertThat(config.jitter()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldRejectNegativeMaxAge() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(-60), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAge must be positive");
    }

    @Test
    void shouldRejectZeroMaxAge() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(Duration.ZERO, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAge must be positive");
    }

    @Test
    void shouldRejectNegativeJitter() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitter must not be negative");
    }

    @Test
    void shouldRejectJitterGreaterThanMaxAge() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(60), Duration.ofSeconds(120)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitter must not be greater than maxAge");
    }
}
