/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Optional;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class MeterRegistries {
    private final PrometheusMeterRegistry prometheusMeterRegistry;

    public MeterRegistries() {
        this.prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    /**
     * Offers up a prometheus registry if available. Currently, we always have a prometheus registry but in
     * future we may wish to use a different micrometer backend. Clients should use the global
     * io.micrometer.core.instrument.Metrics static methods to record metrics, not this implementation. This is used to
     * support specialisations like scraping the prometheus metrics.
     */
    public Optional<PrometheusMeterRegistry> maybePrometheusMeterRegistry() {
        return Optional.ofNullable(prometheusMeterRegistry);
    }
}
