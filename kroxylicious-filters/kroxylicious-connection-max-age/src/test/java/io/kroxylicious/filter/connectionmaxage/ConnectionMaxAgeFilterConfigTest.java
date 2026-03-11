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
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(300, null);
        assertThat(config.maxAgeSeconds()).isEqualTo(300);
        assertThat(config.jitterSeconds()).isNull();
        assertThat(config.maxAgeDuration()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.jitterDuration()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldAcceptValidMaxAgeWithJitter() {
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(300, 30L);
        assertThat(config.maxAgeSeconds()).isEqualTo(300);
        assertThat(config.jitterSeconds()).isEqualTo(30L);
        assertThat(config.maxAgeDuration()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.jitterDuration()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void shouldAcceptZeroJitter() {
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(300, 0L);
        assertThat(config.jitterDuration()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldRejectNegativeMaxAge() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(-60, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAgeSeconds must be positive");
    }

    @Test
    void shouldRejectZeroMaxAge() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAgeSeconds must be positive");
    }

    @Test
    void shouldRejectNegativeJitter() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(300, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitterSeconds must not be negative");
    }

    @Test
    void shouldRejectJitterGreaterThanMaxAge() {
        assertThatThrownBy(() -> new ConnectionMaxAgeFilterConfig(60, 120L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitterSeconds must not be greater than maxAgeSeconds");
    }
}
