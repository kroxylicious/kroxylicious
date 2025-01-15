/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class FeaturesTest {

    public static List<Arguments> enabledByDefault() {
        return List.of(Arguments.of(Feature.TEST_ONLY_CONFIGURATION, false));
    }

    @ParameterizedTest
    @MethodSource
    void enabledByDefault(Feature feature, boolean enabledByDefault) {
        Features features = Features.defaultFeatures();
        assertThat(features.isEnabled(feature)).isEqualTo(enabledByDefault);
    }

    @ParameterizedTest
    @EnumSource(Feature.class)
    void explicitlyEnable(Feature feature) {
        Features features = Features.builder().enable(feature).build();
        assertThat(features.isEnabled(feature)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("enabledByDefault")
    void enabledByDefaultViaBuilder(Feature feature, boolean enabledByDefault) {
        Features features = Features.builder().build();
        assertThat(features.isEnabled(feature)).isEqualTo(enabledByDefault);
    }

    static List<Arguments> supportsValidTestConfiguration() {
        List<Arguments> cases = new ArrayList<>();
        Features testConfigEnabled = Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build();
        cases.add(Arguments.of(testConfigEnabled, Map.of()));
        cases.add(Arguments.of(testConfigEnabled, null));
        cases.add(Arguments.of(testConfigEnabled, Map.of("a", "b")));
        cases.add(Arguments.of(Features.defaultFeatures(), Map.of()));
        cases.add(Arguments.of(Features.defaultFeatures(), null));
        return cases;
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("java:S5738")
    void supportsValidTestConfiguration(Features features, Map<String, Object> config) {
        Configuration configuration = new Configuration(null, null, List.of(), null, false, Optional.ofNullable(config));
        List<String> errorMessages = features.supports(configuration);
        assertThat(errorMessages).isEmpty();
    }

    @Test
    @SuppressWarnings("java:S5738")
    void supportsReturnsErrorOnTestConfigurationPresentWithFeatureDisabled() {
        Optional<Map<String, Object>> a = Optional.of(Map.of("a", "b"));
        Configuration configuration = new Configuration(null, null, List.of(), null, false, a);
        List<String> errors = Features.defaultFeatures().supports(configuration);
        assertThat(errors).containsExactly("test-only configuration for proxy present, but loading test-only configuration not enabled");
    }

    @Test
    void warningsEmptyByDefault() {
        List<String> warnings = Features.defaultFeatures().warnings();
        assertThat(warnings).isEmpty();
    }

    @Test
    void testOnlyConfigurationWarning() {
        Features features = Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build();
        List<String> warnings = features.warnings();
        assertThat(warnings).containsExactly(
                "test-only configuration for proxy will be loaded. "
                        + "these configurations are unsupported and have no compatibility guarantees, "
                        + "they could be removed or changed at any time");
    }

    public static List<Arguments> equalsHashcode() {
        List<Arguments> arguments = new ArrayList<>();
        arguments.add(Arguments.of(Features.defaultFeatures(), Features.defaultFeatures(), true));
        arguments.add(Arguments.of(Features.defaultFeatures(), Features.builder().build(), true));
        arguments.add(Arguments.of(Features.defaultFeatures(), Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build(), false));
        arguments.add(Arguments.of(Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build(), Features.builder().enable(Feature.TEST_ONLY_CONFIGURATION).build(),
                true));
        arguments.add(Arguments.of(Features.defaultFeatures(), "abc", false));
        arguments.add(Arguments.of(Features.defaultFeatures(), null, false));
        return arguments;
    }

    @ParameterizedTest
    @MethodSource
    void equalsHashcode(Object a, Object b, boolean equals) {
        assertThat(a.equals(b)).isEqualTo(equals);
        if (equals) {
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }
    }
}
