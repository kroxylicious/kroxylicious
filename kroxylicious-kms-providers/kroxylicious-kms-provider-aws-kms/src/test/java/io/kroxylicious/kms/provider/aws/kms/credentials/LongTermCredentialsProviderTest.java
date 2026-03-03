/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LongTermCredentialsProviderTest {

    @Test
    void obtainsCredentials() {
        try (var provider = new LongTermCredentialsProvider(new LongTermCredentialsProviderConfig(new InlinePassword("access"), new InlinePassword("secret")))) {
            assertThat(provider.getCredentials())
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isEqualTo(LongTermCredentialsProvider.fixedCredentials("access", "secret"));
        }
    }

    @Test
    void rejectsNullConfig() {
        assertThatThrownBy(() -> new LongTermCredentialsProvider(null))
                .isInstanceOf(NullPointerException.class);
    }
}
