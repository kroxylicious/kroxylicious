/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AbstractDefinitionBuilderTest {

    public static final String TYPE = "concrete";

    record Built(
            String type,
            Map<String, Object> config
    ) {

    }

    private AbstractDefinitionBuilder<Built> builder = new AbstractDefinitionBuilder<>(TYPE) {
        @Override
        protected Built buildInternal(String type, Map<String, Object> config) {
            return new Built(type, config);
        }
    };

    @Test
    void testTwoArgWithConfig() {
        Built build = builder.withConfig("a", "b").build();
        Map<String, String> a = Map.of("a", "b");
        assertBuiltEquals(build, a);
    }

    @Test
    void testFourArgWithConfig() {
        Built build = builder.withConfig("a", "b", "c", "d").build();
        assertBuiltEquals(build, Map.of("a", "b", "c", "d"));
    }

    @Test
    void testSixArgWithConfig() {
        Built build = builder.withConfig("a", "b", "c", "d", "e", "f").build();
        assertBuiltEquals(build, Map.of("a", "b", "c", "d", "e", "f"));
    }

    @Test
    void testEightArgWithConfig() {
        Built build = builder.withConfig("a", "b", "c", "d", "e", "f", "g", "h").build();
        assertBuiltEquals(build, Map.of("a", "b", "c", "d", "e", "f", "g", "h"));
    }

    @Test
    void testMapWithConfig() {
        Built build = builder.withConfig(Map.of("a", "b")).build();
        assertBuiltEquals(build, Map.of("a", "b"));
    }

    @Test
    void testEmptyMapWithConfig() {
        Built build = builder.withConfig(Map.of()).build();
        assertBuiltEquals(build, Map.of());
    }

    @Test
    void testMapWithConfigNull() {
        assertThrows(NullPointerException.class, () -> this.builder.withConfig(null));
    }

    // this helps ensure we generate output YAML deterministically
    @Test
    void testOrderMaintained() {
        Built build = builder.withConfig("a", "b").withConfig("e", "f").withConfig("c", "d").build();
        List<String> keys = build.config.keySet().stream().toList();
        assertThat(keys).containsExactly("a", "e", "c");
    }

    private static void assertBuiltEquals(Built build, Map<String, String> a) {
        assertThat(build.type).isEqualTo(TYPE);
        assertThat(build.config).isEqualTo(a);
    }
}
