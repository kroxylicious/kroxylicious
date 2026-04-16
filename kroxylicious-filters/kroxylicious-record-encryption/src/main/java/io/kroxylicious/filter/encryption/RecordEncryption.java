/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.security.Provider;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.Cipher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.config.EncryptionBufferConfig;
import io.kroxylicious.filter.encryption.config.EncryptionConfigurationException;
import io.kroxylicious.filter.encryption.config.KekSelectorService;
import io.kroxylicious.filter.encryption.config.KmsCacheConfig;
import io.kroxylicious.filter.encryption.config.RecordEncryptionConfig;
import io.kroxylicious.filter.encryption.config.RecordEncryptionConfigV1;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.crypto.EncryptionResolver;
import io.kroxylicious.filter.encryption.decrypt.DecryptionDekCache;
import io.kroxylicious.filter.encryption.decrypt.InBandDecryptionManager;
import io.kroxylicious.filter.encryption.dek.CipherManager;
import io.kroxylicious.filter.encryption.dek.CipherSpecResolver;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionDekCache;
import io.kroxylicious.filter.encryption.encrypt.InBandEncryptionManager;
import io.kroxylicious.filter.encryption.kms.CachingKms;
import io.kroxylicious.filter.encryption.kms.ExponentialJitterBackoffStrategy;
import io.kroxylicious.filter.encryption.kms.InstrumentedKms;
import io.kroxylicious.filter.encryption.kms.KmsMetrics;
import io.kroxylicious.filter.encryption.kms.MicrometerKmsMetrics;
import io.kroxylicious.filter.encryption.kms.ResilientKms;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link FilterFactory} for {@link RecordEncryptionFilter}.
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@Plugin(configVersion = "", configType = RecordEncryptionConfig.class)
@Plugin(configVersion = "v1", configType = RecordEncryptionConfigV1.class)
public class RecordEncryption<K, E> implements FilterFactory<Object, SharedEncryptionContext<K, E>> {

    static final ScheduledExecutorService RETRY_POOL = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread retryThread = new Thread(r, "kmsRetry");
        retryThread.setDaemon(true);
        return retryThread;
    });
    private static final KmsMetrics kmsMetrics = MicrometerKmsMetrics.create(Metrics.globalRegistry);
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordEncryption.class);

    /**
     * Checks that we can build a Cipher for all known CipherSpecs. This prevents us from
     * finding out at decrypt time that we cannot build a Cipher for data encrypted with
     * a known CipherSpec. Instead, we fail early, which stops the Proxy from starting.
     */
    private static void checkCipherSuite() {
        checkCipherSuite(CipherManager::newCipher);
    }

    /* exposed for testing */ static void checkCipherSuite(Function<CipherManager, Cipher> cipherFunc) {
        List<CipherSpec> failures = Arrays.stream(CipherSpec.values()).flatMap(cipherSpec -> {
            try {
                Cipher cipher = cipherFunc.apply(CipherSpecResolver.ALL.fromName(cipherSpec));
                String provider = Optional.ofNullable(cipher.getProvider()).map(Provider::getName).orElse("(no provider)");
                LOGGER.atInfo()
                        .addKeyValue("cipherSpec", cipherSpec)
                        .addKeyValue("provider", provider)
                        .addKeyValue("cipherAlgorithm", cipher.getAlgorithm())
                        .log("Loaded cipher from provider for CipherSpec");
                return Stream.empty();
            }
            catch (Exception e) {
                LOGGER.atError()
                        .addKeyValue("cipherSpec", cipherSpec)
                        .setCause(e)
                        .log("A Cipher could not be constructed for CipherSpec");
                return Stream.of(cipherSpec);
            }
        }).toList();
        if (!failures.isEmpty()) {
            String failedCipherSpecs = failures.stream().map(Enum::name).collect(Collectors.joining(","));
            throw new EncryptionConfigurationException("Cipher Suite check failed, one or more ciphers could not be loaded: " + failedCipherSpecs);
        }
    }

    @Override
    @SuppressWarnings({ "java:S2638", "unchecked", "rawtypes" }) // Tightening UnknownNullness
    public SharedEncryptionContext<K, E> initialize(FilterFactoryContext context,
                                                    @NonNull Object config)
            throws PluginConfigurationException {
        var configuration = Plugins.requireConfig(this, config);
        if (configuration instanceof RecordEncryptionConfigV1 v1) {
            return initializeV1(context, v1);
        }
        else if (configuration instanceof RecordEncryptionConfig legacy) {
            return initializeLegacy(context, legacy);
        }
        throw new PluginConfigurationException("Unsupported config type: " + configuration.getClass().getName());
    }

    @SuppressWarnings("unchecked")
    private SharedEncryptionContext<K, E> initializeLegacy(FilterFactoryContext context,
                                                           RecordEncryptionConfig configuration) {
        LOGGER.atDebug()
                .addKeyValue("encryptionBuffer", configuration.encryptionBuffer())
                .log("Record encryption buffer size configuration");
        checkCipherSuite();
        KmsService<Object, K, E> kmsPlugin = context.pluginInstance(KmsService.class, configuration.kms());
        kmsPlugin.initialize(configuration.kmsConfig());
        return buildSharedContext(configuration, kmsPlugin);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private SharedEncryptionContext<K, E> initializeV1(FilterFactoryContext context,
                                                       RecordEncryptionConfigV1 v1) {
        ResolvedPluginRegistry registry = context.resolvedPluginRegistry()
                .orElseThrow(() -> new PluginConfigurationException(
                        "v1 config requires a ResolvedPluginRegistry but none is available"));
        KmsService kmsPlugin = registry.pluginInstance(KmsService.class, v1.kms());
        Object kmsConfig = registry.pluginConfig(KmsService.class.getName(), v1.kms());
        kmsPlugin.initialize(kmsConfig);

        KekSelectorService selectorPlugin = registry.pluginInstance(KekSelectorService.class, v1.selector());
        Object selectorConfig = registry.pluginConfig(KekSelectorService.class.getName(), v1.selector());

        // Synthesise a RecordEncryptionConfig using implementation class names so that
        // createFilter() can look up plugins from the PluginFactoryRegistry
        var synthesised = new RecordEncryptionConfig(
                kmsPlugin.getClass().getSimpleName(), kmsConfig,
                selectorPlugin.getClass().getSimpleName(), selectorConfig,
                v1.experimental(),
                v1.unresolvedKeyPolicy());

        LOGGER.debug("Record encryption buffer size configuration: {}", synthesised.encryptionBuffer());
        checkCipherSuite();
        return buildSharedContext(synthesised, kmsPlugin);
    }

    private SharedEncryptionContext<K, E> buildSharedContext(RecordEncryptionConfig configuration,
                                                             KmsService<Object, K, E> kmsPlugin) {
        Kms<K, E> kms = buildKms(configuration, kmsPlugin);

        var dekConfig = configuration.dekManager();
        DekManager<K, E> dekManager = new DekManager<>(kms, dekConfig.maxEncryptionsPerDek());

        KmsCacheConfig cacheConfig = configuration.kmsCache();
        EncryptionDekCache<K, E> encryptionDekCache = new EncryptionDekCache<>(dekManager, null, EncryptionDekCache.NO_MAX_CACHE_SIZE,
                cacheConfig.encryptionDekCacheRefreshAfterWriteDuration(), cacheConfig.encryptionDekCacheExpireAfterWriteDuration());
        DecryptionDekCache<K, E> decryptionDekCache = new DecryptionDekCache<>(dekManager, null, DecryptionDekCache.NO_MAX_CACHE_SIZE);
        return new SharedEncryptionContext<>(kms, kmsPlugin::close, configuration, dekManager, encryptionDekCache, decryptionDekCache);
    }

    @NonNull
    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public RecordEncryptionFilter<K> createFilter(FilterFactoryContext context,
                                                  @NonNull SharedEncryptionContext<K, E> sharedEncryptionContext) {
        ScheduledExecutorService filterThreadExecutor = context.filterDispatchExecutor();
        FilterThreadExecutor executor = new FilterThreadExecutor(filterThreadExecutor);
        EncryptionBufferConfig encryptionBufferConfig = sharedEncryptionContext.configuration().encryptionBuffer();
        var encryptionManager = new InBandEncryptionManager<>(Encryption.V2,
                sharedEncryptionContext.dekManager().edekSerde(),
                encryptionBufferConfig.minSizeBytes(),
                encryptionBufferConfig.maxSizeBytes(),
                sharedEncryptionContext.encryptionDekCache(),
                executor);

        var decryptionManager = new InBandDecryptionManager<>(EncryptionResolver.ALL,
                sharedEncryptionContext.dekManager(),
                sharedEncryptionContext.decryptionDekCache(),
                executor);

        RecordEncryptionConfig configuration = sharedEncryptionContext.configuration();
        KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, configuration.selector());
        TopicNameBasedKekSelector<K> kekSelector = ksPlugin.buildSelector(sharedEncryptionContext.kms(), configuration.selectorConfig());
        return new RecordEncryptionFilter<>(encryptionManager, decryptionManager, kekSelector, executor, configuration.unresolvedKeyPolicy());
    }

    @NonNull
    @SuppressWarnings("java:S2245") // Pseudorandomness sufficient for generating backoff jitter; not security relevant
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Pseudorandomness sufficient for generating backoff jitter; not security relevant
    private static <C, K, E> Kms<K, E> buildKms(RecordEncryptionConfig configuration, KmsService<C, K, E> kmsPlugin) {
        Kms<K, E> kms = kmsPlugin.buildKms();
        kms = InstrumentedKms.wrap(kms, kmsMetrics);
        ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                ThreadLocalRandom.current());
        kms = ResilientKms.wrap(kms, RETRY_POOL, backoffStrategy, 3);
        return wrapWithCachingKms(configuration, kms);
    }

    @NonNull
    private static <K, E> Kms<K, E> wrapWithCachingKms(RecordEncryptionConfig configuration, Kms<K, E> resilientKms) {
        KmsCacheConfig config = configuration.kmsCache();
        LOGGER.atDebug()
                .addKeyValue("kmsCacheConfig", config)
                .log("KMS cache configuration");
        return CachingKms.wrap(resilientKms, config.decryptedDekCacheSize(), config.decryptedDekExpireAfterAccessDuration(), config.resolvedAliasCacheSize(),
                config.resolvedAliasExpireAfterWriteDuration(), config.resolvedAliasRefreshAfterWriteDuration(), config.notFoundAliasExpireAfterWriteDuration());
    }

    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public void close(@NonNull SharedEncryptionContext<K, E> initializationData) {
        initializationData.kmsServiceCloser().run();
    }
}
