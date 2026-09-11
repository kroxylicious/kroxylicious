/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouteDescriptorTest {

    private static final TargetCluster TARGET = new TargetCluster("localhost:9092", Optional.empty());

    @Test
    void shouldCreateClusterRoute() {
        var rd = new RouteDescriptor("route-a", 0, TARGET, null, List.of());
        assertThat(rd.targetsCluster()).isTrue();
        assertThat(rd.targetsRouter()).isFalse();
        assertThat(rd.targetCluster()).isSameAs(TARGET);
        assertThat(rd.routerName()).isNull();
        assertThat(rd.name()).isEqualTo("route-a");
        assertThat(rd.filters()).isEmpty();
    }

    @Test
    void shouldCreateRouterRoute() {
        var rd = new RouteDescriptor("route-b", 0, null, "nested-router", List.of());
        assertThat(rd.targetsCluster()).isFalse();
        assertThat(rd.targetsRouter()).isTrue();
        assertThat(rd.routerName()).isEqualTo("nested-router");
        assertThat(rd.targetCluster()).isNull();
    }

    @Test
    void shouldPreserveFilters() {
        var filter = new NamedFilterDefinition("f1", "com.example.MyFilter", null);
        var rd = new RouteDescriptor("route-c", 0, TARGET, null, List.of(filter));
        assertThat(rd.filters()).containsExactly(filter);
    }

    @Test
    void shouldRejectBothTargetClusterAndRouter() {
        assertThatThrownBy(() -> new RouteDescriptor("bad", 0, TARGET, "router", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void shouldRejectNeitherTargetClusterNorRouter() {
        assertThatThrownBy(() -> new RouteDescriptor("bad", 0, null, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void shouldRejectNullName() {
        assertThatThrownBy(() -> new RouteDescriptor(null, 0, TARGET, null, List.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectNullFilters() {
        assertThatThrownBy(() -> new RouteDescriptor("route", 0, TARGET, null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
