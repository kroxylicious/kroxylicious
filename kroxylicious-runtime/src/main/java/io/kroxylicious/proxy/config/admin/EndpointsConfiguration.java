/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import java.util.Optional;

import edu.umd.cs.findbugs.annotations.Nullable;

public record EndpointsConfiguration(@Nullable PrometheusMetricsConfig prometheus) {
    public Optional<PrometheusMetricsConfig> maybePrometheus() {
        return Optional.ofNullable(prometheus);
    }
}
