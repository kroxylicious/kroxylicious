/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouteDefinitionTest {

    private static final RouteTarget CLUSTER_TARGET = new RouteTarget("c1", null);

    @Test
    void shouldCreateValidRoute() {
        var route = new RouteDefinition("route1", 0, null, CLUSTER_TARGET);

        assertThat(route.name()).isEqualTo("route1");
        assertThat(route.id()).isZero();
        assertThat(route.filters()).isNull();
        assertThat(route.target()).isSameAs(CLUSTER_TARGET);
    }

    @Test
    void shouldAcceptFilters() {
        var route = new RouteDefinition("route1", 0, List.of("filter1", "filter2"), CLUSTER_TARGET);

        assertThat(route.filters()).containsExactly("filter1", "filter2");
    }

    @Test
    void clusterAccessorDelegatesToTarget() {
        var route = new RouteDefinition("route1", 0, null, new RouteTarget("my-cluster", null));

        assertThat(route.cluster()).isEqualTo("my-cluster");
        assertThat(route.router()).isNull();
    }

    @Test
    void routerAccessorDelegatesToTarget() {
        var route = new RouteDefinition("route1", 0, null, new RouteTarget(null, "my-router"));

        assertThat(route.cluster()).isNull();
        assertThat(route.router()).isEqualTo("my-router");
    }

    @Test
    void shouldRejectNullName() {
        assertThatThrownBy(() -> new RouteDefinition(null, 0, null, CLUSTER_TARGET))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void shouldRejectNullTarget() {
        assertThatThrownBy(() -> new RouteDefinition("route1", 0, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("target");
    }

    @Test
    void shouldRejectNegativeId() {
        assertThatThrownBy(() -> new RouteDefinition("route1", -1, null, CLUSTER_TARGET))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("invalid id -1")
                .hasMessageContaining("must be >= 0");
    }

    @Test
    void shouldAcceptZeroId() {
        assertThatCode(() -> new RouteDefinition("route1", 0, null, CLUSTER_TARGET))
                .doesNotThrowAnyException();
    }

}
