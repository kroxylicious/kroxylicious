/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TargetClusterTest {

    @Test
    void shouldRejectEmptyBootstrapServers() {
        Optional<Tls> empty = Optional.empty();
        assertThatThrownBy(() -> new TargetCluster(null, empty))
                .isInstanceOf(IllegalArgumentException.class);
    }
}