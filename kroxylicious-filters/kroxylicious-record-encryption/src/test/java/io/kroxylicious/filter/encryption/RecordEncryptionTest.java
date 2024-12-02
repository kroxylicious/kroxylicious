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
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.crypto.Cipher;

import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.config.DekManagerConfig;
import io.kroxylicious.filter.encryption.config.EncryptionConfigurationException;
import io.kroxylicious.filter.encryption.config.KekSelectorService;
import io.kroxylicious.filter.encryption.config.KmsCacheConfig;
import io.kroxylicious.filter.encryption.config.RecordEncryptionConfig;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.dek.DekException;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RecordEncryptionTest {

    static Cipher arbitraryCipher = aesCipher();

    @NonNull
    private static Cipher aesCipher() {
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static @NonNull KmsCacheConfig getKmsCacheConfig(String key, Object value) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(key, value);
        RecordEncryptionConfig config = createConfig(map);
        return RecordEncryption.kmsCache(config);
    }

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

    static Stream<Arguments> invalidExperimentalConfig() {
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
    void invalidExperimentalConfig(String configKey, Object configValue) {
        RecordEncryptionConfig config = createConfig(Map.of(configKey, configValue));
        assertThatThrownBy(() -> RecordEncryption.kmsCache(config)).isInstanceOf(IllegalArgumentException.class);
    }

    private static @NonNull RecordEncryptionConfig createConfig(Map<String, Object> map) {
        return new RecordEncryptionConfig("kms", 1L, "selector", 2L, map);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldInitAndCreateFilter() {
        var kmsConfig = new Object();
        var config = new RecordEncryptionConfig("KMS", kmsConfig, "SELECTOR", null, null);
        var recordEncryption = new RecordEncryption<>();
        var fc = mock(FilterFactoryContext.class);
        var kmsService = mock(KmsService.class);
        var kms = mock(Kms.class);
        var kekSelectorService = mock(KekSelectorService.class);
        var kekSelector = mock(TopicNameBasedKekSelector.class);
        var edekSerde = mock(Serde.class);

        doReturn(kmsService).when(fc).pluginInstance(KmsService.class, "KMS");
        doReturn(kms).when(kmsService).buildKms();
        doReturn(mock(FilterDispatchExecutor.class)).when(fc).filterDispatchExecutor();
        doReturn(edekSerde).when(kms).edekSerde();

        doReturn(kekSelectorService).when(fc).pluginInstance(KekSelectorService.class, "SELECTOR");
        doReturn(kekSelector).when(kekSelectorService).buildSelector(any(), any());

        var sec = recordEncryption.initialize(fc, config);
        var filter = recordEncryption.createFilter(fc, sec);
        assertThat(filter).isNotNull();
        verify(kmsService).initialize(kmsConfig);
        recordEncryption.close(sec);
    }

    @Test
    void closePropagatedToKmsService() {
        var kmsConfig = new Object();
        var config = new RecordEncryptionConfig("KMS", kmsConfig, "SELECTOR", null, null);
        var recordEncryption = new RecordEncryption<>();
        var fc = mock(FilterFactoryContext.class);
        var kmsService = mock(KmsService.class);
        var kms = mock(Kms.class);

        doReturn(kmsService).when(fc).pluginInstance(KmsService.class, "KMS");
        doReturn(kms).when(kmsService).buildKms();

        var sec = recordEncryption.initialize(fc, config);
        recordEncryption.close(sec);

        verify(kmsService).close();
    }

    @Test
    void testKmsCacheConfigDefaults() {
        KmsCacheConfig config = RecordEncryption.kmsCache(new RecordEncryptionConfig("vault", 1L, "selector", 1L, null));
        assertThat(config.decryptedDekCacheSize()).isEqualTo(1000);
        assertThat(config.decryptedDekExpireAfterAccessDuration()).isEqualTo(Duration.ofHours(1));
        assertThat(config.resolvedAliasCacheSize()).isEqualTo(1000);
        assertThat(config.resolvedAliasExpireAfterWriteDuration()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.resolvedAliasRefreshAfterWriteDuration()).isEqualTo(Duration.ofMinutes(8));
        assertThat(config.notFoundAliasExpireAfterWriteDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.encryptionDekCacheRefreshAfterWriteDuration()).isEqualTo(Duration.ofHours(1));
        assertThat(config.encryptionDekCacheExpireAfterWriteDuration()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void testDekManagerConfigDefaults() {
        var config = RecordEncryption.dekManager(new RecordEncryptionConfig("vault", 1L, "selector", 1L, null));
        assertThat(config.maxEncryptionsPerDek()).isEqualTo(5_000_000L);
    }

    @Test
    void testKmsCacheConfigDefaultsWhenPropertiesNull() {
        Map<String, Object> experimental = new HashMap<>();
        experimental.put("decryptedDekCacheSize", null);
        experimental.put("decryptedDekExpireAfterAccessSeconds", null);
        experimental.put("resolvedAliasCacheSize", null);
        experimental.put("resolvedAliasExpireAfterWriteSeconds", null);
        experimental.put("resolvedAliasRefreshAfterWriteSeconds", null);
        experimental.put("notFoundAliasExpireAfterWriteSeconds", null);
        experimental.put("encryptionDekRefreshAfterWriteSeconds", null);
        experimental.put("encryptionDekExpireAfterWriteSeconds", null);
        KmsCacheConfig config = RecordEncryption.kmsCache(new RecordEncryptionConfig("vault", 1L, "selector", 1L,
                experimental));
        assertThat(config.decryptedDekCacheSize()).isEqualTo(1000);
        assertThat(config.decryptedDekExpireAfterAccessDuration()).isEqualTo(Duration.ofHours(1));
        assertThat(config.resolvedAliasCacheSize()).isEqualTo(1000);
        assertThat(config.resolvedAliasExpireAfterWriteDuration()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.resolvedAliasRefreshAfterWriteDuration()).isEqualTo(Duration.ofMinutes(8));
        assertThat(config.notFoundAliasExpireAfterWriteDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.encryptionDekCacheRefreshAfterWriteDuration()).isEqualTo(Duration.ofHours(1));
        assertThat(config.encryptionDekCacheExpireAfterWriteDuration()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void testDekManagerConfigDefaultsWhenPropertiesNull() {
        Map<String, Object> experimental = new HashMap<>();
        experimental.put("maxEncryptionsPerDek", null);

        var config = RecordEncryption.dekManager(new RecordEncryptionConfig("vault", 1L, "selector", 1L, null));
        assertThat(config.maxEncryptionsPerDek()).isEqualTo(5_000_000L);
    }

    @Test
    void testKmsCacheConfigOverrides() {
        KmsCacheConfig kmsCacheConfig = new KmsCacheConfig(
                1,
                Duration.ofSeconds(2L),
                3,
                Duration.ofSeconds(4L),
                Duration.ofSeconds(5L),
                Duration.ofSeconds(6L),
                Duration.ofSeconds(7L),
                Duration.ofSeconds(8L));

        Map<String, Object> experimental = new HashMap<>();
        experimental.put("decryptedDekCacheSize", 1);
        experimental.put("decryptedDekExpireAfterAccessSeconds", 2);
        experimental.put("resolvedAliasCacheSize", 3);
        experimental.put("resolvedAliasExpireAfterWriteSeconds", 4);
        experimental.put("resolvedAliasRefreshAfterWriteSeconds", 5);
        experimental.put("notFoundAliasExpireAfterWriteSeconds", 6);
        experimental.put("encryptionDekRefreshAfterWriteSeconds", 7);
        experimental.put("encryptionDekExpireAfterWriteSeconds", 8);
        KmsCacheConfig config = RecordEncryption.kmsCache(new RecordEncryptionConfig("vault", 1L, "selector", 1L, experimental));
        assertThat(config).isEqualTo(kmsCacheConfig);
    }

    @Test
    void testDekManagerConfigOverrides() {
        var dekManagerCacheConfig = new DekManagerConfig(
                1_000L);

        Map<String, Object> experimental = new HashMap<>();
        experimental.put("maxEncryptionsPerDek", 1_000L);

        var config = RecordEncryption.dekManager(new RecordEncryptionConfig("vault", 1L, "selector", 1L, experimental));
        assertThat(config).isEqualTo(dekManagerCacheConfig);
    }

    @Test
    void testRetryPool() {
        Future<Thread> thread = RecordEncryption.RETRY_POOL.submit(Thread::currentThread);
        assertThat(thread).succeedsWithin(Duration.ofSeconds(5)).satisfies(thread1 -> {
            assertThat(thread1.getName()).isEqualTo("kmsRetry");
            assertThat(thread1.isDaemon()).isTrue();
        });
    }

    @Test
    void checkCipherSuiteFailure() {
        AbstractThrowableAssert<?, ? extends Throwable> throwableAssert = assertThatThrownBy(() -> {
            RecordEncryption.checkCipherSuite(cipherSpec -> {
                throw new DekException("Could not construct cipher for " + cipherSpec);
            });
        }).isInstanceOf(EncryptionConfigurationException.class);
        throwableAssert.hasMessageContaining("Cipher Suite check failed, one or more ciphers could not be loaded");
        for (CipherSpec value : CipherSpec.values()) {
            throwableAssert.hasMessageContaining(value.name());
        }
    }

    @Test
    void checkCipherSuiteSuccess() {
        assertThatCode(() -> RecordEncryption.checkCipherSuite(cipherSpec -> arbitraryCipher)).doesNotThrowAnyException();
    }

}
