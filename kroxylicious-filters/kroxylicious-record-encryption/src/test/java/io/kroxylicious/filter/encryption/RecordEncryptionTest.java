/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

import javax.crypto.Cipher;

import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

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

    @Test
    @SuppressWarnings("unchecked")
    void shouldInitAndCreateFilter() {
        var kmsConfig = new Object();
        var config = new RecordEncryptionConfig("KMS", kmsConfig, "SELECTOR", null, null, Optional.empty());
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
        var config = new RecordEncryptionConfig("KMS", kmsConfig, "SELECTOR", null, null, Optional.empty());
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
        KmsCacheConfig config = new RecordEncryptionConfig("vault", 1L, "selector", 1L, null, Optional.empty()).kmsCache();
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
        var config = new RecordEncryptionConfig("vault", 1L, "selector", 1L, null, Optional.empty()).dekManager();
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
        KmsCacheConfig config = new RecordEncryptionConfig("vault", 1L, "selector", 1L,
                experimental, Optional.empty()).kmsCache();
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

        var config = new RecordEncryptionConfig("vault", 1L, "selector", 1L, null, Optional.empty()).dekManager();
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
        KmsCacheConfig config = new RecordEncryptionConfig("vault", 1L, "selector", 1L, experimental, Optional.empty()).kmsCache();
        assertThat(config).isEqualTo(kmsCacheConfig);
    }

    @Test
    void testDekManagerConfigOverrides() {
        var dekManagerCacheConfig = new DekManagerConfig(
                1_000L);

        Map<String, Object> experimental = new HashMap<>();
        experimental.put("maxEncryptionsPerDek", 1_000L);

        var config = new RecordEncryptionConfig("vault", 1L, "selector", 1L, experimental, Optional.empty()).dekManager();
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
