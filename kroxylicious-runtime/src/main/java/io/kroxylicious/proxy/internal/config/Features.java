/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.config;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;

import static java.util.Arrays.stream;

/**
 * Represents the entire set of proxy features.
 */
public class Features {

    private static final Features DEFAULT_FEATURES = new Features(Map.of());
    private final Map<Feature, Boolean> featureToEnabled;

    private Features(Map<Feature, Boolean> featureToEnabled) {
        this.featureToEnabled = featureToEnabled;
    }

    /**
     * Check whether this configuration is supported by the proxy's features
     * @param configuration configuration
     * @return if not supported, returns a list of errors, else an empty list
     */
    public List<String> supports(Configuration configuration) {
        return stream(Feature.values())
                .flatMap(feature -> feature.supports(configuration, isEnabled(feature)))
                .toList();
    }

    /**
     *
     * @param feature feature to test
     * @return true if feature enabled, else false
     */
    public boolean isEnabled(Feature feature) {
        return featureToEnabled.getOrDefault(feature, feature.enabledByDefault());
    }

    public static Features defaultFeatures() {
        return DEFAULT_FEATURES;
    }

    /**
     * If any sensitive or unsupported features are enabled, they may return a warning to be logged
     * @return list of warnings, empty if no warnings
     */
    public List<String> warnings() {
        return stream(Feature.values()).flatMap(feature -> feature.maybeWarning(isEnabled(feature)).stream()).toList();
    }

    public static FeaturesBuilder builder() {
        return new FeaturesBuilder();
    }

    public static class FeaturesBuilder {
        private final Map<Feature, Boolean> features = new EnumMap<>(Feature.class);

        public FeaturesBuilder enable(Feature feature) {
            features.put(feature, true);
            return this;
        }

        public Features build() {
            if (features.isEmpty()) {
                return Features.defaultFeatures();
            }
            return new Features(Collections.unmodifiableMap(features));
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Features features1 = (Features) o;
        return Objects.equals(featureToEnabled, features1.featureToEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(featureToEnabled);
    }

    @Override
    public String toString() {
        return stream(Feature.values()).map(f -> f.name() + ": " + (isEnabled(f) ? "enabled" : "disabled")).collect(Collectors.joining(",", "[", "]"));
    }
}
