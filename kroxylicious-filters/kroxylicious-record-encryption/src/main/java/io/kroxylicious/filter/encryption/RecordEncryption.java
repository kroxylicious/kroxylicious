/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.inband.DecryptionDekCache;
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
import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Objects.requireNonNullElse;

/**
 * A {@link FilterFactory} for {@link RecordEncryptionFilter}.
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@Plugin(configType = RecordEncryption.Config.class)
public class RecordEncryption<K, E> implements FilterFactory<RecordEncryption.Config, SharedEncryptionContext<K, E>> {

    static final ScheduledExecutorService RETRY_POOL = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread retryThread = new Thread(r, "kmsRetry");
        retryThread.setDaemon(true);
        return retryThread;
    });
    private static KmsMetrics kmsMetrics = MicrometerKmsMetrics.create(Metrics.globalRegistry);
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordEncryption.class);

    record Config(
                  @JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                  @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                  @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,
                  @PluginImplConfig(implNameProperty = "selector") Object selectorConfig,
                  @JsonProperty Map<String, Object> experimental) {
        Config {
            experimental = experimental == null ? Map.of() : experimental;
        }

        KmsCacheConfig kmsCache() {
            Integer decryptedDekCacheSize = getExperimentalInt("decryptedDekCacheSize");
            Long decryptedDekExpireAfterAccessSeconds = getExperimentalLong("decryptedDekExpireAfterAccessSeconds");
            Integer resolvedAliasCacheSize = getExperimentalInt("resolvedAliasCacheSize");
            Long resolvedAliasExpireAfterWriteSeconds = getExperimentalLong("resolvedAliasExpireAfterWriteSeconds");
            Long resolvedAliasRefreshAfterWriteSeconds = getExperimentalLong("resolvedAliasRefreshAfterWriteSeconds");
            Long notFoundAliasExpireAfterWriteSeconds = getExperimentalLong("notFoundAliasExpireAfterWriteSeconds");
            return new KmsCacheConfig(decryptedDekCacheSize, decryptedDekExpireAfterAccessSeconds, resolvedAliasCacheSize, resolvedAliasExpireAfterWriteSeconds,
                    resolvedAliasRefreshAfterWriteSeconds, notFoundAliasExpireAfterWriteSeconds);
        }

        @Nullable
        private Integer getExperimentalInt(String property) {
            if (experimental.containsKey(property)) {
                Object value = experimental.get(property);
                if (value instanceof Number number) {
                    return number.intValue();
                }
            }
            return null;
        }

        @Nullable
        private Long getExperimentalLong(String property) {
            if (experimental.containsKey(property)) {
                Object value = experimental.get(property);
                if (value instanceof Number number) {
                    return number.longValue();
                }
            }
            return null;
        }

    }

    record KmsCacheConfig(
                          Integer decryptedDekCacheSize,
                          Duration decryptedDekExpireAfterAccessDuration,
                          Integer resolvedAliasCacheSize,
                          Duration resolvedAliasExpireAfterWriteDuration,
                          Duration resolvedAliasRefreshAfterWriteDuration,
                          Duration notFoundAliasExpireAfterWriteDuration) {

        KmsCacheConfig {
            decryptedDekCacheSize = requireNonNullElse(decryptedDekCacheSize, 1000);
            decryptedDekExpireAfterAccessDuration = requireNonNullElse(decryptedDekExpireAfterAccessDuration, Duration.ofHours(1));
            resolvedAliasCacheSize = requireNonNullElse(resolvedAliasCacheSize, 1000);
            resolvedAliasExpireAfterWriteDuration = requireNonNullElse(resolvedAliasExpireAfterWriteDuration, Duration.ofMinutes(10));
            resolvedAliasRefreshAfterWriteDuration = requireNonNullElse(resolvedAliasRefreshAfterWriteDuration, Duration.ofMinutes(8));
            notFoundAliasExpireAfterWriteDuration = requireNonNullElse(notFoundAliasExpireAfterWriteDuration, Duration.ofSeconds(30));
        }

        KmsCacheConfig(Integer decryptedDekCacheSize,
                       Long decryptedDekExpireAfterAccessSeconds,
                       Integer resolvedAliasCacheSize,
                       Long resolvedAliasExpireAfterWriteSeconds,
                       Long resolvedAliasRefreshAfterWriteSeconds,
                       Long notFoundAliasExpireAfterWriteSeconds) {
            this(mapNotNull(decryptedDekCacheSize, Function.identity()),
                    (Duration) mapNotNull(decryptedDekExpireAfterAccessSeconds, Duration::ofSeconds),
                    mapNotNull(resolvedAliasCacheSize, Function.identity()),
                    mapNotNull(resolvedAliasExpireAfterWriteSeconds, Duration::ofSeconds),
                    mapNotNull(resolvedAliasRefreshAfterWriteSeconds, Duration::ofSeconds),
                    mapNotNull(notFoundAliasExpireAfterWriteSeconds, Duration::ofSeconds));
        }

        static <T, Y> Y mapNotNull(T t, Function<T, Y> function) {
            return t == null ? null : function.apply(t);
        }

        private static final KmsCacheConfig DEFAULT_CONFIG = new KmsCacheConfig(null, (Long) null, null, null, null, null);
    }

    @Override
    public SharedEncryptionContext<K, E> initialize(FilterFactoryContext context,
                                                    Config configuration)
            throws PluginConfigurationException {
        Kms<K, E> kms = buildKms(context, configuration);

        DekManager<K, E> dekManager = new DekManager<>(ignored -> kms, null, 5_000_000);
        EncryptionDekCache<K, E> encryptionDekCache = new EncryptionDekCache<>(dekManager, null, EncryptionDekCache.NO_MAX_CACHE_SIZE);
        DecryptionDekCache<K, E> decryptionDekCache = new DecryptionDekCache<>(dekManager, null, DecryptionDekCache.NO_MAX_CACHE_SIZE);
        return new SharedEncryptionContext<>(kms, configuration, dekManager, encryptionDekCache, decryptionDekCache);
    }

    @NonNull
    @Override
    public RecordEncryptionFilter<K> createFilter(FilterFactoryContext context,
                                                  SharedEncryptionContext<K, E> sharedEncryptionContext) {

        ScheduledExecutorService filterThreadExecutor = context.eventLoop();
        FilterThreadExecutor executor = new FilterThreadExecutor(filterThreadExecutor);
        var encryptionManager = new InBandEncryptionManager<>(sharedEncryptionContext.dekManager().edekSerde(),
                1024 * 1024,
                8 * 1024 * 1024,
                sharedEncryptionContext.encryptionDekCache(),
                executor);

        var decryptionManager = new InBandDecryptionManager<>(sharedEncryptionContext.dekManager(),
                sharedEncryptionContext.decryptionDekCache(),
                executor);

        KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, sharedEncryptionContext.configuration().selector());
        TopicNameBasedKekSelector<K> kekSelector = ksPlugin.buildSelector(sharedEncryptionContext.kms(), sharedEncryptionContext.configuration().selectorConfig());
        return new RecordEncryptionFilter<>(encryptionManager, decryptionManager, kekSelector, executor);
    }

    @NonNull
    @SuppressWarnings("java:S2245") // secure randomization not needed for exponential backoff
    private static <K, E> Kms<K, E> buildKms(FilterFactoryContext context, Config configuration) {
        KmsService<Object, K, E> kmsPlugin = context.pluginInstance(KmsService.class, configuration.kms());
        Kms<K, E> kms = kmsPlugin.buildKms(configuration.kmsConfig());
        kms = InstrumentedKms.wrap(kms, kmsMetrics);
        ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                ThreadLocalRandom.current());
        kms = ResilientKms.wrap(kms, RETRY_POOL, backoffStrategy, 3);
        return wrapWithCachingKms(configuration, kms);
    }

    @NonNull
    private static <K, E> Kms<K, E> wrapWithCachingKms(Config configuration, Kms<K, E> resilientKms) {
        KmsCacheConfig config = configuration.kmsCache();
        LOGGER.debug("KMS cache configuration: {}", config);
        return CachingKms.wrap(resilientKms, config.decryptedDekCacheSize, config.decryptedDekExpireAfterAccessDuration, config.resolvedAliasCacheSize,
                config.resolvedAliasExpireAfterWriteDuration, config.resolvedAliasRefreshAfterWriteDuration, config.notFoundAliasExpireAfterWriteDuration);
    }
}
