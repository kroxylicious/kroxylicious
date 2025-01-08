/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.net.URI;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.fortanix.dsm.config.ApiKeySessionProviderConfig;
import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static io.kroxylicious.kms.provider.fortanix.dsm.session.SessionProviderFactory.DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SessionProviderFactoryTest {

    @Test
    void constructsProvider() {
        var config = new Config(URI.create("https://localhost"), new ApiKeySessionProviderConfig(new InlinePassword("key"), null), null);
        try (var sessionProvider = DEFAULT.createSessionProvider(config)) {
            assertThat(sessionProvider).isNotNull();
        }
    }

    @Test
    void rejectsNullConfig() {
        assertThatThrownBy(() -> DEFAULT.createSessionProvider(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void missingConfig() {
        var config = new Config(URI.create("https://localhost"), null, null);
        assertThatThrownBy(() -> DEFAULT.createSessionProvider(config))
                .isInstanceOf(KmsException.class);
    }
}
