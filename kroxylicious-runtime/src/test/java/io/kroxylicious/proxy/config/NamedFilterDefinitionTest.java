/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NamedFilterDefinitionTest {

    @Test
    void shouldAcceptValidNames() {
        // Should work without throwing
        new NamedFilterDefinition("a", "", "");
        new NamedFilterDefinition("1", "", "");
        new NamedFilterDefinition("a1", "", "");
        new NamedFilterDefinition("a1.b2", "", "");
        new NamedFilterDefinition("a1.B2-C3.d4", "", "");
        new NamedFilterDefinition("io.kroxylicious.proxy.internal.filter.OptionalConfigFactory", "", "");
    }

    @Test
    void shouldRejectInvalidNames() {
        assertThatThrownBy(() -> new NamedFilterDefinition("", "", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid filter name '' (should match '[a-z0-9A-Z](?:[a-z0-9A-Z_.-]{0,251}[a-z0-9A-Z])?')");
        assertThatThrownBy(() -> new NamedFilterDefinition("x".repeat(254), "", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NamedFilterDefinition(".foo", "", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NamedFilterDefinition("-foo", "", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}