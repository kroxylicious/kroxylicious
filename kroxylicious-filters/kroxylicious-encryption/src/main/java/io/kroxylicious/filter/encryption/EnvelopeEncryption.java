/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
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
public class EnvelopeEncryption<K, E> implements FilterFactory<EnvelopeEncryption.Config, EnvelopeEncryption.ApplicationWideState<K, E>> {

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

    public record FilterState<K, E>(ApplicationWideState<K, E> applicationWide, PerEventLoopState<K, E> perEventLoop) {}

    public record ApplicationWideState<K, E>(Kms<K, E> kms, Map<ScheduledExecutorService, PerEventLoopState<K, E>> stateMap, Config config) {

        ApplicationWideState(FilterFactoryContext context, Config config) {
            this(createKms(context, config), Collections.synchronizedMap(new IdentityHashMap<>()), config);
        }

        private static <K, E> Kms<K, E> createKms(FilterFactoryContext context, Config configuration) {
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

        FilterState<K, E> stateFor(FilterFactoryContext context) {
            PerEventLoopState<K, E> perEventLoopState = stateMap.computeIfAbsent(context.eventLoop(), exec -> new PerEventLoopState<>(context, this));
            return new FilterState<>(this, perEventLoopState);
        }
    }

    public record PerEventLoopState<K, E>(KeyManager<K> keyManager, TopicNameBasedKekSelector<K> selector) {
        PerEventLoopState(FilterFactoryContext context, ApplicationWideState<K, E> applicationWideState) {
            this(createKeyManager(applicationWideState), createKekSelector(context, applicationWideState, applicationWideState.config()));
        }

        @NonNull
        private static <K, E> TopicNameBasedKekSelector<K> createKekSelector(FilterFactoryContext context, ApplicationWideState<K, E> applicationWideState,
                                                                             Config configuration) {
            KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, configuration.selector());
            return ksPlugin.buildSelector(applicationWideState.kms(), configuration.selectorConfig());
        }

        @NonNull
        private static <K, E> KeyManager<K> createKeyManager(ApplicationWideState<K, E> applicationWideState) {
            return new InBandKeyManager<>(applicationWideState.kms(), BufferPool.allocating(), 500_000);
        }
    }

    @Override
    public ApplicationWideState<K, E> initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return new ApplicationWideState<>(context, config);
    }

    @NonNull
    @Override
    public EnvelopeEncryptionFilter<K> createFilter(FilterFactoryContext context, ApplicationWideState<K, E> applicationWideState) {
        FilterState<K, E> filterState = applicationWideState.stateFor(context);
        return new EnvelopeEncryptionFilter<>(filterState.perEventLoop().keyManager(), filterState.perEventLoop().selector());
    }
}
