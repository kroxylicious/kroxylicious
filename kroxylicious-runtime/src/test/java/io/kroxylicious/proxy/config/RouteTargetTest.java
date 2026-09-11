/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouteTargetTest {

    @Test
    void shouldAcceptClusterOnly() {
        var target = new RouteTarget("my-cluster", null);

        assertThat(target.cluster()).isEqualTo("my-cluster");
        assertThat(target.router()).isNull();
    }

    @Test
    void shouldAcceptRouterOnly() {
        var target = new RouteTarget(null, "my-router");

        assertThat(target.cluster()).isNull();
        assertThat(target.router()).isEqualTo("my-router");
    }

    @Test
    void shouldRejectBothProvided() {
        assertThatThrownBy(() -> new RouteTarget("cluster", "router"))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("both were provided");
    }

    @Test
    void shouldRejectNeitherProvided() {
        assertThatThrownBy(() -> new RouteTarget(null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("neither was provided");
    }
}
