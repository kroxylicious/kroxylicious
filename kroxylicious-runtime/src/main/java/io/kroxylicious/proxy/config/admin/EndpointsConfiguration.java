/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import java.util.Optional;

import edu.umd.cs.findbugs.annotations.Nullable;

public record EndpointsConfiguration(
                                     @Nullable PrometheusMetricsConfig prometheus,
                                     @Nullable ConfigReloadEndpointConfig configReload) {

    public Optional<PrometheusMetricsConfig> maybePrometheus() {
        return Optional.ofNullable(prometheus);
    }

    /**
     * Get config reload endpoint configuration, returning disabled config if not specified.
     */
    public ConfigReloadEndpointConfig configReload() {
        return configReload != null ? configReload : ConfigReloadEndpointConfig.createDisabled();
    }
}
