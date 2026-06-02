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

class RouterDefinitionTest {

    private static RouteDefinition route(String name, int id) {
        return new RouteDefinition(name, id, null, new RouteTarget("c1", null));
    }

    @Test
    void shouldCreateValidDefinition() {
        var def = new RouterDefinition("r1", "MyRouterFactory", null, List.of(route("route1", 0)));

        assertThat(def.name()).isEqualTo("r1");
        assertThat(def.type()).isEqualTo("MyRouterFactory");
        assertThat(def.config()).isNull();
        assertThat(def.routes()).hasSize(1);
    }

    @Test
    void shouldRejectNullName() {
        assertThatThrownBy(() -> new RouterDefinition(null, "type", null, List.of(route("r", 0))))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void shouldRejectNullType() {
        assertThatThrownBy(() -> new RouterDefinition("r1", null, null, List.of(route("r", 0))))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("type");
    }

    @Test
    void shouldRejectNullRoutes() {
        assertThatThrownBy(() -> new RouterDefinition("r1", "type", null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must have at least one route");
    }

    @Test
    void shouldRejectEmptyRoutes() {
        assertThatThrownBy(() -> new RouterDefinition("r1", "type", null, List.of()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must have at least one route");
    }

    @Test
    void shouldRejectDuplicateRouteNames() {
        var routes = List.of(route("dup", 0), route("dup", 1));

        assertThatThrownBy(() -> new RouterDefinition("r1", "type", null, routes))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("duplicate route names")
                .hasMessageContaining("dup");
    }

    @Test
    void shouldAcceptMultipleUniqueRoutes() {
        var routes = List.of(route("a", 0), route("b", 1));

        assertThatCode(() -> new RouterDefinition("r1", "type", null, routes))
                .doesNotThrowAnyException();
    }
}
