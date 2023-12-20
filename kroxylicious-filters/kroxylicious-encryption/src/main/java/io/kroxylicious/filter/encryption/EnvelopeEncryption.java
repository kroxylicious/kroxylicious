/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.filter.encryption.inband.BufferPool;
import io.kroxylicious.filter.encryption.inband.InBandKeyManager;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link FilterFactory} for {@link EnvelopeEncryptionFilter}.
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@Plugin(configType = EnvelopeEncryption.Config.class)
public class EnvelopeEncryption<K, E> implements FilterFactory<EnvelopeEncryption.Config, EnvelopeEncryption.Config> {

    private static KmsMetrics kmsMetrics = MicrometerKmsMetrics.create(Metrics.globalRegistry);

    record Config(
                  @JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                  @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                  @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,
                  @PluginImplConfig(implNameProperty = "selector") Object selectorConfig) {

    }

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return config;
    }

    @NonNull
    @Override
    @SuppressWarnings("java:S2245") // secure randomization not needed for exponential backoff
    public EnvelopeEncryptionFilter<K> createFilter(FilterFactoryContext context, Config configuration) {
        KmsService<Object, K, E> kmsPlugin = context.pluginInstance(KmsService.class, configuration.kms());
        Kms<K, E> kms = kmsPlugin.buildKms(configuration.kmsConfig());
        kms = InstrumentedKms.instrument(kms, kmsMetrics);

        ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                ThreadLocalRandom.current());
        Kms<K, E> resilientKms = ResilientKms.get(kms, context.eventLoop(), backoffStrategy, 3);
        Kms<K, E> resilientCachingKms = CachingKms.caching(resilientKms, 1000, Duration.ofHours(1), 1000, Duration.ofMinutes(10), Duration.ofMinutes(8));

        var keyManager = new InBandKeyManager<>(resilientCachingKms, BufferPool.allocating(), 500_000);

        KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, configuration.selector());
        TopicNameBasedKekSelector<K> kekSelector = ksPlugin.buildSelector(kms, configuration.selectorConfig());
        return new EnvelopeEncryptionFilter<>(keyManager, kekSelector);
    }
}
