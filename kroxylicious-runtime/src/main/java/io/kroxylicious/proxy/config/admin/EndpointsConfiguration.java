/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import java.util.Optional;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configures the endpoints exposed by the management HTTP server.
 *
 * @param prometheus if present, a Prometheus scrape endpoint is exposed
 */
public record EndpointsConfiguration(@Nullable PrometheusMetricsConfig prometheus) {
    /**
     * The Prometheus endpoint configuration, if one was configured.
     *
     * @return an optional containing the Prometheus configuration, or empty if the endpoint is not enabled
     */
    public Optional<PrometheusMetricsConfig> maybePrometheus() {
        return Optional.ofNullable(prometheus);
    }
}
