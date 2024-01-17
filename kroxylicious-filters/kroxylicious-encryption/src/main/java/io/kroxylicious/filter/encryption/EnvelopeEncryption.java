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
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

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
public class EnvelopeEncryption<K, E> implements FilterFactory<EnvelopeEncryption.Config, EnvelopeEncryption.FilterFactoryInstanceScopedState<K, E>> {

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

    public static final class FilterFactoryInstanceScopedState<K, E> {
        private final Kms<K, E> kms;
        private final Map<ScheduledExecutorService, PerFilterThreadState<K, E>> stateMap;
        private final Config config;
        private final DekAllocator<K, E> dekAllocator;

        public FilterFactoryInstanceScopedState(Kms<K, E> kms, Map<ScheduledExecutorService, PerFilterThreadState<K, E>> stateMap, Config config) {
            this.kms = kms;
            this.stateMap = stateMap;
            this.config = config;
            this.dekAllocator = new DekAllocator<>(kms, (long) Math.pow(2, 32));
        }

        FilterFactoryInstanceScopedState(FilterFactoryContext context, Config config) {
            this(createKms(context, config), Collections.synchronizedMap(new IdentityHashMap<>()), config);
        }

        @SuppressWarnings("java:S2245") // secure randomization not needed for exponential backoff
        private static <K, E> Kms<K, E> createKms(FilterFactoryContext context, Config configuration) {
            KmsService<Object, K, E> kmsPlugin = context.pluginInstance(KmsService.class, configuration.kms());
            Kms<K, E> kms = kmsPlugin.buildKms(configuration.kmsConfig());
            kms = InstrumentedKms.wrap(kms, kmsMetrics);
            ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                    new Random());
            kms = ResilientKms.wrap(kms, context.eventLoop(), backoffStrategy, 3);
            return wrapWithCachingKms(configuration, kms);
        }

        @NonNull
        private static <K, E> Kms<K, E> wrapWithCachingKms(Config configuration, Kms<K, E> resilientKms) {
            KmsCacheConfig config = configuration.kmsCache();
            return CachingKms.wrap(resilientKms, config.decryptedDekCacheSize, config.decryptedDekExpireAfterAccessDuration, config.resolvedAliasCacheSize,
                    config.resolvedAliasExpireAfterWriteDuration, config.resolvedAliasRefreshAfterWriteDuration);
        }

        PerFilterThreadState<K, E> stateFor(FilterFactoryContext context) {
            ScheduledExecutorService eventLoop = context.eventLoop();
            if (eventLoop == null) {
                throw new IllegalStateException("eventloop on context is null");
            }
            return stateMap.computeIfAbsent(eventLoop, exec -> new PerFilterThreadState<>(context, this));
        }

        public Kms<K, E> kms() {
            return kms;
        }

        public Config config() {
            return config;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (FilterFactoryInstanceScopedState) obj;
            return Objects.equals(this.kms, that.kms) &&
                    Objects.equals(this.stateMap, that.stateMap) &&
                    Objects.equals(this.config, that.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(kms, stateMap, config);
        }

        @Override
        public String toString() {
            return "ApplicationWideState[" +
                    "kms=" + kms + ", " +
                    "stateMap=" + stateMap + ", " +
                    "config=" + config + ']';
        }

    }

    public record PerFilterThreadState<K, E>(KeyManager<K> keyManager, TopicNameBasedKekSelector<K> selector) {
        PerFilterThreadState(FilterFactoryContext context, FilterFactoryInstanceScopedState<K, E> filterFactoryInstanceState) {
            this(createKeyManager(filterFactoryInstanceState), createKekSelector(context, filterFactoryInstanceState, filterFactoryInstanceState.config()));
        }

        @NonNull
        private static <K, E> TopicNameBasedKekSelector<K> createKekSelector(FilterFactoryContext context,
                                                                             FilterFactoryInstanceScopedState<K, E> filterFactoryInstanceState,
                                                                             Config configuration) {
            KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, configuration.selector());
            return ksPlugin.buildSelector(filterFactoryInstanceState.kms(), configuration.selectorConfig());
        }

        @NonNull
        private static <K, E> KeyManager<K> createKeyManager(FilterFactoryInstanceScopedState<K, E> filterFactoryInstanceState) {
            return new InBandKeyManager<>(filterFactoryInstanceState.kms(), BufferPool.allocating(), 500_000, filterFactoryInstanceState.dekAllocator);
        }
    }

    @Override
    public FilterFactoryInstanceScopedState<K, E> initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return new FilterFactoryInstanceScopedState<>(context, config);
    }

    @NonNull
    @Override
    public EnvelopeEncryptionFilter<K> createFilter(FilterFactoryContext context, FilterFactoryInstanceScopedState<K, E> filterFactoryInstanceState) {
        PerFilterThreadState<K, E> filterState = filterFactoryInstanceState.stateFor(context);
        return new EnvelopeEncryptionFilter<>(filterState.keyManager(), filterState.selector());
    }
}
