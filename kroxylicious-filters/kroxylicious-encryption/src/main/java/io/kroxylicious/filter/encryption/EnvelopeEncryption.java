/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.filter.encryption.dek.DekManager;
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
public class EnvelopeEncryption<K, E> implements FilterFactory<EnvelopeEncryption.Config, EnvelopeEncryption.Config> {

    private static KmsMetrics kmsMetrics = MicrometerKmsMetrics.create(Metrics.globalRegistry);

    record Config(
                  @JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                  @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                  @Deprecated @JsonProperty @PluginImplName(KekSelectorService.class) String selector,

                  @Deprecated @PluginImplConfig(implNameProperty = "selector") Object selectorConfig,

                  List<TopicConfiguration> topics) {

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

    public record TopicConfiguration(List<String> includedTopicNames,
                                     List<String> excludedTopicNames,
                                     List<String> includedTopicPrefixes,
                                     List<String> excludedTopicPrefixes,

                                     @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,

                                     @PluginImplConfig(implNameProperty = "selector") Object selectorConfig) {

        @Override
        public List<String> includedTopicNames() {
            return emptyIfNull(includedTopicNames);
        }

        @Override
        public List<String> excludedTopicNames() {
            return emptyIfNull(excludedTopicNames);
        }

        @Override
        public List<String> includedTopicPrefixes() {
            return emptyIfNull(includedTopicPrefixes);
        }

        @Override
        public List<String> excludedTopicPrefixes() {
            return emptyIfNull(excludedTopicPrefixes);
        }

        @NonNull
        private static List<String> emptyIfNull(List<String> in) {
            return in == null ? List.of() : in;
        }

    }

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return config;
    }

    @NonNull
    @Override
    public EnvelopeEncryptionFilter<K> createFilter(FilterFactoryContext context, Config configuration) {
        Kms<K, E> kms = buildKms(context, configuration);

        DekManager<K, E> dekManager = new DekManager<>(ignored -> kms, null, 5_000_000);
        ScheduledExecutorService filterThreadExecutor = context.eventLoop();
        FilterThreadExecutor executor = new FilterThreadExecutor(filterThreadExecutor);
        var encryptionManager = new InBandEncryptionManager<>(dekManager,
                1024 * 1024,
                8 * 1024 * 1024,
                null,
                executor,
                InBandEncryptionManager.NO_MAX_CACHE_SIZE);
        var decryptionManager = new InBandDecryptionManager<>(kms, executor);

        TopicNameRouter<TopicNameBasedKekSelector<K>> router;
        if (configuration.topics != null) {
            router = handleTopicRoutingConfig(context, configuration, kms);
        }
        else if (configuration.selector != null) {
            router = handleDeprecatedConfig(context, configuration, kms);
        }
        else {
            throw new IllegalStateException("configuration should contain 'topics' or deprecated 'selector'");
        }
        return new EnvelopeEncryptionFilter<>(encryptionManager, decryptionManager, router, executor);
    }

    private static <K, E> TopicNameRouter<TopicNameBasedKekSelector<K>> handleTopicRoutingConfig(FilterFactoryContext context, Config configuration,
                                                                                                 Kms<K, E> kms) {
        TopicNameBasedKekSelector<K> excludeByDefault = new TopicNameBasedKekSelector<>() {
            @NonNull
            @Override
            public CompletionStage<Map<String, K>> selectKek(@NonNull Set<String> topicNames) {
                HashMap<String, K> result = new HashMap<>();
                for (String topicName : topicNames) {
                    result.put(topicName, null);
                }
                return CompletableFuture.completedFuture(result);
            }
        };
        // note: router depends on equals method of the TopicNameBasedKekSelector so that it can build the map
        // Map<TopicNameBasedKekSelector<K>, Set<String>> route(Set<String> topicName) later.
        TopicNameRouter.Builder<TopicNameBasedKekSelector<K>> builder = TopicNameRouter.builder(excludeByDefault);
        for (TopicConfiguration topic : configuration.topics) {
            KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, topic.selector());
            TopicNameBasedKekSelector<K> kekSelector = ksPlugin.buildSelector(kms, topic.selectorConfig());
            // the order of inclusions and exclusions added to the builder matters, includes can negate prior excludes
            topic.includedTopicNames().forEach(topicName -> builder.addIncludeExactNameRoute(topicName, kekSelector));
            topic.includedTopicPrefixes().forEach(prefix -> builder.addIncludePrefixRoute(prefix, kekSelector));
            topic.excludedTopicNames().forEach(builder::addExcludeExactNameRoute);
            topic.excludedTopicPrefixes().forEach(builder::addExcludePrefixRoute);
        }
        return builder.build();
    }

    private static <K, E> TopicNameRouter<TopicNameBasedKekSelector<K>> handleDeprecatedConfig(FilterFactoryContext context, Config configuration,
                                                                                               Kms<K, E> kms) {
        TopicNameRouter<TopicNameBasedKekSelector<K>> router;
        KekSelectorService<Object, K> ksPlugin = context.pluginInstance(KekSelectorService.class, configuration.selector());
        TopicNameBasedKekSelector<K> kekSelector = ksPlugin.buildSelector(kms, configuration.selectorConfig());
        router = TopicNameRouter.builder(kekSelector).build();
        return router;
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
