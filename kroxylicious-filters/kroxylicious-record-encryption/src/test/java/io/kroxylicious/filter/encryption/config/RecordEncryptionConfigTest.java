/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.json.JsonMapper;

import io.kroxylicious.filter.encryption.RecordEncryption;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class RecordEncryptionConfigTest {

    static Stream<Arguments> experimentalConfig() {
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
    void experimentalConfig(String configKey, Object configValue, Object expectedKmsConfig, Function<KmsCacheConfig, Object> accessor) {
        assertThat(accessor.apply(getKmsCacheConfig(configKey, configValue))).isEqualTo(expectedKmsConfig);
    }

    private static @NonNull KmsCacheConfig getKmsCacheConfig(String key, Object value) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(key, value);
        RecordEncryptionConfig config = createConfig(map);
        return RecordEncryption.kmsCache(config);
    }

    private static @NonNull RecordEncryptionConfig createConfig(Map<String, Object> map) {
        var mapper = new JsonMapper();
        RecordEncryptionConfigExperimental recordEncryptionConfigExperimental = mapper.convertValue(map, RecordEncryptionConfigExperimental.class);
        return new RecordEncryptionConfig("kms", 1L, "selector", 2L, recordEncryptionConfigExperimental);
    }

}
