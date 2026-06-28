/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class RecordEncryptionConfigTest {

    public static final int DEFAULT_MAX_BUFFER_SIZE = 1024 * 1024 * 8;
    public static final int DEFAULT_MIN_BUFFER_SIZE = 1024 * 1024;

    static Stream<Arguments> experimentalKmsCacheConfig() {
        return Stream.of(
                Arguments.of("decryptedDekCacheSize", 2, 2, (Function<KmsCacheConfig, Integer>) KmsCacheConfig::decryptedDekCacheSize),
                Arguments.of("decryptedDekCacheSize", "3", 3, (Function<KmsCacheConfig, Integer>) KmsCacheConfig::decryptedDekCacheSize),
                Arguments.of("decryptedDekCacheSize", null, 1000, (Function<KmsCacheConfig, Integer>) KmsCacheConfig::decryptedDekCacheSize),
                Arguments.of("decryptedDekExpireAfterAccessSeconds", 4L, Duration.ofSeconds(4),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::decryptedDekExpireAfterAccessDuration),
                Arguments.of("decryptedDekExpireAfterAccessSeconds", "5", Duration.ofSeconds(5),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::decryptedDekExpireAfterAccessDuration),
                Arguments.of("decryptedDekExpireAfterAccessSeconds", null, Duration.ofHours(1),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::decryptedDekExpireAfterAccessDuration),
                Arguments.of("resolvedAliasCacheSize", 2, 2, (Function<KmsCacheConfig, Integer>) KmsCacheConfig::resolvedAliasCacheSize),
                Arguments.of("resolvedAliasCacheSize", 3, 3, (Function<KmsCacheConfig, Integer>) KmsCacheConfig::resolvedAliasCacheSize),
                Arguments.of("resolvedAliasCacheSize", null, 1000, (Function<KmsCacheConfig, Integer>) KmsCacheConfig::resolvedAliasCacheSize),
                Arguments.of("resolvedAliasExpireAfterWriteSeconds", 4L, Duration.ofSeconds(4),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::resolvedAliasExpireAfterWriteDuration),
                Arguments.of("resolvedAliasExpireAfterWriteSeconds", "5", Duration.ofSeconds(5),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::resolvedAliasExpireAfterWriteDuration),
                Arguments.of("resolvedAliasExpireAfterWriteSeconds", null, Duration.ofMinutes(10),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::resolvedAliasExpireAfterWriteDuration),
                Arguments.of("resolvedAliasRefreshAfterWriteSeconds", 4L, Duration.ofSeconds(4),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::resolvedAliasRefreshAfterWriteDuration),
                Arguments.of("resolvedAliasRefreshAfterWriteSeconds", "5", Duration.ofSeconds(5),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::resolvedAliasRefreshAfterWriteDuration),
                Arguments.of("resolvedAliasRefreshAfterWriteSeconds", null, Duration.ofMinutes(8),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::resolvedAliasRefreshAfterWriteDuration),
                Arguments.of("notFoundAliasExpireAfterWriteSeconds", 4L, Duration.ofSeconds(4),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::notFoundAliasExpireAfterWriteDuration),
                Arguments.of("notFoundAliasExpireAfterWriteSeconds", "5", Duration.ofSeconds(5),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::notFoundAliasExpireAfterWriteDuration),
                Arguments.of("notFoundAliasExpireAfterWriteSeconds", null, Duration.ofSeconds(30),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::notFoundAliasExpireAfterWriteDuration),
                Arguments.of("encryptionDekRefreshAfterWriteSeconds", 4L, Duration.ofSeconds(4),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::encryptionDekCacheRefreshAfterWriteDuration),
                Arguments.of("encryptionDekRefreshAfterWriteSeconds", "5", Duration.ofSeconds(5),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::encryptionDekCacheRefreshAfterWriteDuration),
                Arguments.of("encryptionDekRefreshAfterWriteSeconds", null, Duration.ofHours(1),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::encryptionDekCacheRefreshAfterWriteDuration),
                Arguments.of("encryptionDekExpireAfterWriteSeconds", 4L, Duration.ofSeconds(4),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::encryptionDekCacheExpireAfterWriteDuration),
                Arguments.of("encryptionDekExpireAfterWriteSeconds", "5", Duration.ofSeconds(5),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::encryptionDekCacheExpireAfterWriteDuration),
                Arguments.of("encryptionDekExpireAfterWriteSeconds", null, Duration.ofHours(2),
                        (Function<KmsCacheConfig, Duration>) KmsCacheConfig::encryptionDekCacheExpireAfterWriteDuration));
    }

    @ParameterizedTest(name = "{0} - {1}")
    @MethodSource
    void experimentalKmsCacheConfig(String configKey, Object configValue, Object expectedKmsConfig, Function<KmsCacheConfig, Object> accessor) {
        assertThat(accessor.apply(getKmsCacheConfig(configKey, configValue))).isEqualTo(expectedKmsConfig);
    }

    static Stream<Arguments> invalidExperimentalKmsCacheConfig() {
        return Stream.of(
                Arguments.of("decryptedDekCacheSize", List.of()),
                Arguments.of("decryptedDekCacheSize", "banana"),
                Arguments.of("decryptedDekExpireAfterAccessSeconds", List.of()),
                Arguments.of("decryptedDekExpireAfterAccessSeconds", "banana"),
                Arguments.of("resolvedAliasCacheSize", List.of()),
                Arguments.of("resolvedAliasCacheSize", "banana"),
                Arguments.of("resolvedAliasExpireAfterWriteSeconds", List.of()),
                Arguments.of("resolvedAliasExpireAfterWriteSeconds", "banana"),
                Arguments.of("resolvedAliasRefreshAfterWriteSeconds", List.of()),
                Arguments.of("resolvedAliasRefreshAfterWriteSeconds", "banana"),
                Arguments.of("notFoundAliasExpireAfterWriteSeconds", List.of()),
                Arguments.of("notFoundAliasExpireAfterWriteSeconds", "banana"),
                Arguments.of("encryptionDekRefreshAfterWriteSeconds", List.of()),
                Arguments.of("encryptionDekRefreshAfterWriteSeconds", "banana"),
                Arguments.of("encryptionDekExpireAfterWriteSeconds", List.of()),
                Arguments.of("encryptionDekExpireAfterWriteSeconds", "banana"));
    }

    @ParameterizedTest(name = "{0} - {1}")
    @MethodSource
    void invalidExperimentalKmsCacheConfig(String configKey, Object configValue) {
        RecordEncryptionConfig config = createConfig(Map.of(configKey, configValue));
        assertThatThrownBy(config::kmsCache).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> invalidExperimentalEncryptionBufferConfig() {
        return Stream.of(argumentSet("min size greater than default max", Map.of("encryptionBufferMinimumSizeBytes", "9000000")),
                argumentSet("min size zero", Map.of("encryptionBufferMinimumSizeBytes", "0")),
                argumentSet("min size negative", Map.of("encryptionBufferMinimumSizeBytes", "-1")),
                argumentSet("max size less min", Map.of("encryptionBufferMinimumSizeBytes", "3", "encryptionBufferMaximumSizeBytes", "2")),
                argumentSet("max size less than default min", Map.of("encryptionBufferMaximumSizeBytes", "" + (DEFAULT_MIN_BUFFER_SIZE - 1))),
                argumentSet("max size zero", Map.of("encryptionBufferMaximumSizeBytes", "0")),
                argumentSet("max size negative", Map.of("encryptionBufferMaximumSizeBytes", "-1")));
    }

    @ParameterizedTest()
    @MethodSource
    void invalidExperimentalEncryptionBufferConfig(Map<String, Object> configMap) {
        RecordEncryptionConfig config = createConfig(configMap);
        assertThatThrownBy(config::encryptionBuffer).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> validExperimentalEncryptionBufferConfig() {
        return Stream.of(argumentSet("min size set", Map.of("encryptionBufferMinimumSizeBytes", "500"), 500, DEFAULT_MAX_BUFFER_SIZE),
                argumentSet("max size set", Map.of("encryptionBufferMaximumSizeBytes", "2000000"), DEFAULT_MIN_BUFFER_SIZE, 2000000),
                argumentSet("both set", Map.of("encryptionBufferMinimumSizeBytes", "1", "encryptionBufferMaximumSizeBytes", "2"), 1, 2),
                argumentSet("equal minimum and maximum", Map.of("encryptionBufferMinimumSizeBytes", "1", "encryptionBufferMaximumSizeBytes", "1"), 1, 1));
    }

    @ParameterizedTest()
    @MethodSource
    void validExperimentalEncryptionBufferConfig(Map<String, Object> configMap, int expectedMinSize, int expectedMaxSize) {
        RecordEncryptionConfig config = createConfig(configMap);
        EncryptionBufferConfig buffer = config.encryptionBuffer();
        assertThat(buffer.minSizeBytes()).isEqualTo(expectedMinSize);
        assertThat(buffer.maxSizeBytes()).isEqualTo(expectedMaxSize);
    }

    @Test
    void defaultUnresolvedEncryptionPolicy() {
        RecordEncryptionConfig config = new RecordEncryptionConfig("kms", 1L, "selector", 2L, Map.of(), null);
        assertThat(config.unresolvedKeyPolicy()).isEqualTo(UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
    }

    @Test
    void specifiedUnresolvedEncryptionPolicy() {
        RecordEncryptionConfig config = new RecordEncryptionConfig("kms", 1L, "selector", 2L, Map.of(), UnresolvedKeyPolicy.REJECT);
        assertThat(config.unresolvedKeyPolicy()).isEqualTo(UnresolvedKeyPolicy.REJECT);
    }

    private static @NonNull KmsCacheConfig getKmsCacheConfig(String key, Object value) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(key, value);
        RecordEncryptionConfig config = createConfig(map);
        return config.kmsCache();
    }

    private static @NonNull RecordEncryptionConfig createConfig(Map<String, Object> map) {
        return new RecordEncryptionConfig("kms", 1L, "selector", 2L, map, UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);
    }

}