/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.aws.kms.config.FixedCredentialsProviderConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedCredentialsProviderTest {

    @Test
    void obtainsCredentials() {
        try (var provider = new FixedCredentialsProvider(new FixedCredentialsProviderConfig(new InlinePassword("access"), new InlinePassword("secret")))) {
            assertThat(provider.getCredentials())
                    .succeedsWithin(Duration.ofSeconds(1))
                    .isEqualTo(FixedCredentialsProvider.fixedCredentials("access", "secret"));
        }
    }

    @Test
    void rejectsNullConfig() {
        assertThatThrownBy(() -> new FixedCredentialsProvider(null))
                .isInstanceOf(NullPointerException.class);
    }
}
