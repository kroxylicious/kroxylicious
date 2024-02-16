/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.inband.EncryptionDekCache;
import io.kroxylicious.filter.encryption.inband.InBandDecryptionManager;
import io.kroxylicious.filter.encryption.inband.InBandEncryptionManager;
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
public class EnvelopeEncryption<K, E> implements FilterFactory<EnvelopeEncryption.Config, SharedEncryptionContext<K, E>> {

    private static KmsMetrics kmsMetrics = MicrometerKmsMetrics.create(Metrics.globalRegistry);

    record Config(
                  @JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                  @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                  @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,
                  @PluginImplConfig(implNameProperty = "selector") Object selectorConfig) {

        public KmsCacheConfig kmsCache() {
            return KmsCacheConfig.DEFAULT_CONFIG;
        }
    }

    record KmsCacheConfig(
                          int decryptedDekCacheSize,
                          @NonNull Duration decryptedDekExpireAfterAccessDuration,
                          int resolvedAliasCacheSize,
                          @NonNull Duration resolvedAliasExpireAfterWriteDuration,
                          @NonNull Duration resolvedAliasRefreshAfterWriteDuration) {

        private static final KmsCacheConfig DEFAULT_CONFIG = new KmsCacheConfig(1000, Duration.ofHours(1), 1000, Duration.ofMinutes(10), Duration.ofMinutes(8));

    }

    @Override
    public SharedEncryptionContext<K, E> initialize(FilterFactoryContext context,
                                                    Config configuration)
            throws PluginConfigurationException {
        Kms<K, E> kms = buildKms(context, configuration);

        DekManager<K, E> dekManager = new DekManager<>(ignored -> kms, null, 5_000_000);
        EncryptionDekCache<K, E> encryptionDekCache = new EncryptionDekCache<>(dekManager, null, EncryptionDekCache.NO_MAX_CACHE_SIZE);
        return new SharedEncryptionContext<>(kms, configuration, dekManager, encryptionDekCache);
    }

    @NonNull
    @Override
    public EnvelopeEncryptionFilter<K> createFilter(FilterFactoryContext context,
                                                    SharedEncryptionContext<K, E> sharedEncryptionContext) {

        ScheduledExecutorService filterThreadExecutor = context.eventLoop();
        FilterThreadExecutor executor = new FilterThreadExecutor(filterThreadExecutor);
        var encryptionManager = new InBandEncryptionManager<K, E>(sharedEncryptionContext.dekManager().edekSerde(),
                1024 * 1024,
                8 * 1024 * 1024,
                sharedEncryptionContext.encryptionDekCache(),
                executor);

        var decryptionManager = new InBandDecryptionManager<>(sharedEncryptionContext.dekManager(),
                executor,
                null,
                InBandDecryptionManager.NO_MAX_CACHE_SIZE);

        KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, sharedEncryptionContext.configuration().selector());
        TopicNameBasedKekSelector<K> kekSelector = ksPlugin.buildSelector(sharedEncryptionContext.kms(), sharedEncryptionContext.configuration().selectorConfig());
        return new EnvelopeEncryptionFilter<>(encryptionManager, decryptionManager, kekSelector, executor);
    }

    @NonNull
    @SuppressWarnings("java:S2245") // secure randomization not needed for exponential backoff
    private static <K, E> Kms<K, E> buildKms(FilterFactoryContext context, Config configuration) {
        KmsService<Object, K, E> kmsPlugin = context.pluginInstance(KmsService.class, configuration.kms());
        Kms<K, E> kms = kmsPlugin.buildKms(configuration.kmsConfig());
        kms = InstrumentedKms.wrap(kms, kmsMetrics);
        ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                ThreadLocalRandom.current());
        kms = ResilientKms.wrap(kms, context.eventLoop(), backoffStrategy, 3);
        return wrapWithCachingKms(configuration, kms);
    }

    @NonNull
    private static <K, E> Kms<K, E> wrapWithCachingKms(Config configuration, Kms<K, E> resilientKms) {
        KmsCacheConfig config = configuration.kmsCache();
        return CachingKms.wrap(resilientKms, config.decryptedDekCacheSize, config.decryptedDekExpireAfterAccessDuration, config.resolvedAliasCacheSize,
                config.resolvedAliasExpireAfterWriteDuration, config.resolvedAliasRefreshAfterWriteDuration);
    }
}
