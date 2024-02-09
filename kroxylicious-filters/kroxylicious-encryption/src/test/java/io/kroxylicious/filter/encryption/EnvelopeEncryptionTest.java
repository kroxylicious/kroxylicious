/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class EnvelopeEncryptionTest {

    @Test
    void shouldInitAndCreateFilter() {
        EnvelopeEncryption.Config config = new EnvelopeEncryption.Config("KMS", null, "SELECTOR", null);
        var ee = new EnvelopeEncryption<>();
        var fc = mock(FilterFactoryContext.class);
        var kmsService = mock(KmsService.class);
        var kms = mock(Kms.class);
        var kekSelectorService = mock(KekSelectorService.class);
        var kekSelector = mock(TopicNameBasedKekSelector.class);

        doReturn(kmsService).when(fc).pluginInstance(KmsService.class, "KMS");
        doReturn(kms).when(kmsService).buildKms(any());
        doReturn(mock(ScheduledExecutorService.class)).when(fc).eventLoop();

        doReturn(kekSelectorService).when(fc).pluginInstance(KekSelectorService.class, "SELECTOR");
        doReturn(kekSelector).when(kekSelectorService).buildSelector(any(), any());

        ee.initialize(fc, config);
        var filter = ee.createFilter(fc, config);
        assertNotNull(filter);
    }

    @Test
    void testKmsCacheConfigDefaults() {
        EnvelopeEncryption.KmsCacheConfig config = new EnvelopeEncryption.Config("vault", 1L, "selector", 1L).kmsCache();
        assertThat(config.decryptedDekCacheSize()).isEqualTo(1000);
        assertThat(config.decryptedDekExpireAfterAccessDuration()).isEqualTo(Duration.ofHours(1));
        assertThat(config.resolvedAliasCacheSize()).isEqualTo(1000);
        assertThat(config.resolvedAliasExpireAfterWriteDuration()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.resolvedAliasRefreshAfterWriteDuration()).isEqualTo(Duration.ofMinutes(8));
    }
}
