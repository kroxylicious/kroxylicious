/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookContributorManager;

public class MeterRegistries {
    private final PrometheusMeterRegistry prometheusMeterRegistry;

    public MeterRegistries(List<MicrometerDefinition> micrometerConfig) {
        configureMicrometer(micrometerConfig);
        this.prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    private void configureMicrometer(List<MicrometerDefinition> micrometerConfig) {
        CompositeMeterRegistry globalRegistry = Metrics.globalRegistry;
        MicrometerConfigurationHookContributorManager manager = MicrometerConfigurationHookContributorManager.getInstance();
        micrometerConfig
                .stream()
                .map(f -> manager.getHook(f.type(), f.config()))
                .forEach(micrometerConfigurationHook -> micrometerConfigurationHook.configure(globalRegistry));
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
