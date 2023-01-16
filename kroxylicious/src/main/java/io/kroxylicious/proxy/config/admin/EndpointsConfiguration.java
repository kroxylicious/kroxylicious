/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import java.util.Optional;

public class EndpointsConfiguration {
    private final PrometheusMetricsConfig prometheus;

    public EndpointsConfiguration(PrometheusMetricsConfig prometheus) {
        this.prometheus = prometheus;
    }

    public Optional<PrometheusMetricsConfig> maybePrometheus() {
        return Optional.ofNullable(prometheus);
    }
}
