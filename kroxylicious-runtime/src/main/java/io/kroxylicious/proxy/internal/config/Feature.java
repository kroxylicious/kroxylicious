/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.config;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.kroxylicious.proxy.config.Configuration;

/**
 * Configuration Features related to how we load configuration.
 */
public enum Feature {

    /**
     * Enables loading specific nodes in the configuration file which configure test-only features. These are unsupported
     * for production usages and there are no compatibility guarantees for their configuration model.
     */
    TEST_ONLY_CONFIGURATION {
        @Override
        public Optional<String> maybeWarning(boolean enabled) {
            if (enabled) {
                return Optional.of(
                        "test-only configuration for proxy will be loaded. these configurations are unsupported and have no compatibility guarantees, they could be removed or changed at any time");
            }
            else {
                return Optional.empty();
            }
        }

        @Override
        public Stream<String> supports(Configuration configuration, boolean enabled) {
            Optional<Map<String, Object>> development = configuration.development();
            if (development.isPresent() && !development.get().isEmpty() && !enabled) {
                return Stream.of("test-only configuration for proxy present, but loading test-only configuration not enabled");
            }
            return Stream.empty();
        }
    };

    /**
     * Check if a configuration is supported by this feature
     * @param configuration proxy configuration
     * @param enabled true if this feature is enabled
     * @return a stream of error messages, a non-empty stream indicates the configuration is unsupported
     */
    public abstract Stream<String> supports(Configuration configuration, boolean enabled);

    public boolean enabledByDefault() {
        return false;
    }

    /**
     * At proxy initialisation time, this method will be invoked to give the feature an opportunity to log
     * any disclaimers.
     * @param enabled true if this feature is enabled
     */
    public abstract Optional<String> maybeWarning(boolean enabled);
}
