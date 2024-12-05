/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import io.kroxylicious.proxy.config.Configuration;

import edu.umd.cs.findbugs.annotations.NonNull;

public enum ProxyEnvironment {
    PRODUCTION {
        @Override
        public Configuration validate(@NonNull Configuration config) {
            if (config.internal().map(m -> !m.isEmpty()).orElse(false)) {
                throw new EnvironmentConfigurationException("internal configuration for proxy present in production environment");
            }
            return config;
        }
    },
    DEVELOPMENT;

    /**
     *
     * @param config configuration to validate
     * @return the config object passed in if valid
     * @throws EnvironmentConfigurationException if config invalid for environment
     */
    public Configuration validate(Configuration config) {
        return config;
    }
}
