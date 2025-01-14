/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHook;
import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookService;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class MeterRegistries implements AutoCloseable {
    private final PrometheusMeterRegistry prometheusMeterRegistry;

    private static final Logger logger = LoggerFactory.getLogger(MeterRegistries.class);
    private final PluginFactoryRegistry pfr;
    private final List<MicrometerConfigurationHook> hooks;

    public MeterRegistries(PluginFactoryRegistry pfr, List<MicrometerDefinition> micrometerConfig) {
        this.pfr = pfr;
        this.hooks = registerHooks(micrometerConfig);
        this.prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    private List<MicrometerConfigurationHook> registerHooks(List<MicrometerDefinition> micrometerConfig) {
        CompositeMeterRegistry globalRegistry = Metrics.globalRegistry;
        preventDifferentTagNameRegistration(globalRegistry);
        var configurationHooks = micrometerConfig.stream()
                .map(f -> pfr.pluginFactory(MicrometerConfigurationHookService.class).pluginInstance(f.type()).build(f.config()))
                .toList();
        configurationHooks.forEach(micrometerConfigurationHook -> micrometerConfigurationHook.configure(globalRegistry));
        return configurationHooks;
    }

    /**
     * By default, when we register multiple meters with the same name but different tag names,
     * only the data for one of those meters can be scraped at the prometheus endpoint.
     * This is a limitation of the prometheus client. So we add a filter that explodes when we
     * attempt to register a metric with a different set of tags.
     *
     * @param registry registry
     */
    @VisibleForTesting
    static void preventDifferentTagNameRegistration(CompositeMeterRegistry registry) {
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                boolean allTagsSame = registry.find(id.getName()).meters().stream().allMatch(meter -> tagNames(meter.getId()).equals(tagNames(id)));
                if (!allTagsSame) {
                    logger.error("Attempted to register a meter with id {} which is already registered but with a different set of tag names", id);
                    throw new IllegalArgumentException("tags for id " + id + " differ from existing meters registered");
                }
                return MeterFilterReply.ACCEPT;
            }
        });
    }

    private static Object tagNames(Meter.Id id1) {
        return id1.getTags().stream().map(Tag::getKey).collect(Collectors.toSet());
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

    @Override
    public void close() {
        hooks.forEach(MicrometerConfigurationHook::close);
        // remove the meters we contributed to the global registry.
        var copy = List.copyOf(prometheusMeterRegistry.getMeters());
        copy.forEach(Metrics.globalRegistry::remove);
        Metrics.removeRegistry(prometheusMeterRegistry);
    }
}
