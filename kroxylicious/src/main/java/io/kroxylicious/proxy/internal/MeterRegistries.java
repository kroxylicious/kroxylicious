/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.lang.reflect.Constructor;
import java.util.Optional;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import io.kroxylicious.proxy.config.micrometer.MicrometerConfiguration;
import io.kroxylicious.proxy.config.micrometer.MicrometerConfigurationHook;

public class MeterRegistries {
    private final PrometheusMeterRegistry prometheusMeterRegistry;

    public MeterRegistries(MicrometerConfiguration micrometerConfig) {
        configureMicrometer(micrometerConfig);
        this.prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    private void configureMicrometer(MicrometerConfiguration micrometerConfig) {
        CompositeMeterRegistry globalRegistry = Metrics.globalRegistry;
        configureCommonTags(micrometerConfig, globalRegistry);
        configureBinders(micrometerConfig, globalRegistry);
        callConfigurationHook(micrometerConfig, globalRegistry);
    }

    private void configureCommonTags(MicrometerConfiguration micrometerConfig, CompositeMeterRegistry targetRegistry) {
        targetRegistry.config().commonTags(micrometerConfig.getCommonTags());
    }

    private static void callConfigurationHook(MicrometerConfiguration micrometerConfig, CompositeMeterRegistry targetRegistry) {
        Optional<Class<? extends MicrometerConfigurationHook>> configurationHookClass = micrometerConfig.loadConfigurationHookClass();
        if (configurationHookClass.isPresent()) {
            Class<? extends MicrometerConfigurationHook> clazz = configurationHookClass.get();
            MicrometerConfigurationHook micrometerConfigurationHook = instantiateWithNoArgs(clazz);
            micrometerConfigurationHook.configure(targetRegistry);
        }
    }

    private static void configureBinders(MicrometerConfiguration micrometerConfig, CompositeMeterRegistry targetRegistry) {
        for (Class<? extends MeterBinder> binder : micrometerConfig.getBinders()) {
            MeterBinder meterBinder = instantiateWithNoArgs(binder);
            try {
                meterBinder.bindTo(targetRegistry);
            }
            catch (Exception e) {
                throw new RuntimeException("failed to bind metrics with " + binder.getSimpleName(), e);
            }
        }
    }

    private static <T> T instantiateWithNoArgs(Class<? extends T> binder) {
        try {
            Constructor<? extends T> declaredConstructor = binder.getDeclaredConstructor();
            return declaredConstructor.newInstance();
        }
        catch (Exception e) {
            throw new IllegalArgumentException("failed to instantiate " + binder.getSimpleName(), e);
        }
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
