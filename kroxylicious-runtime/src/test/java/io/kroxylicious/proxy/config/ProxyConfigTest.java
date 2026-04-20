/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProxyConfigTest {

    @Test
    void shouldAcceptNullDrainTimeoutAndDefault() {
        assertThatCode(() -> new ProxyConfig(null)).doesNotThrowAnyException();
        assertThat(new ProxyConfig(null).drainTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void shouldPreserveUserSuppliedDrainTimeout() {
        assertThat(new ProxyConfig(Duration.ofSeconds(5)).drainTimeout()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void shouldAcceptPositiveDrainTimeout() {
        assertThatCode(() -> new ProxyConfig(Duration.ofMillis(1))).doesNotThrowAnyException();
        assertThatCode(() -> new ProxyConfig(Duration.ofSeconds(60))).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectZeroDrainTimeout() {
        assertThatThrownBy(() -> new ProxyConfig(Duration.ZERO))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("proxy.drainTimeout must be positive");
    }

    @Test
    void shouldRejectNegativeDrainTimeout() {
        assertThatThrownBy(() -> new ProxyConfig(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("proxy.drainTimeout must be positive");
    }
}
