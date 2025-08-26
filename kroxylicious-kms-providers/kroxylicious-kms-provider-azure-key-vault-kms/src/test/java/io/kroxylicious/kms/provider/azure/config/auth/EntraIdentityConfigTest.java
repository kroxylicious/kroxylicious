/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import java.net.URI;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;

class EntraIdentityConfigTest {

    public static final String CLIENT_ID = "abc";
    public static final String CLIENT_SECRET = "def";
    public static final String TENANT_ID = "tenant";

    @Test
    void minimumAuthConfiguration() {
        EntraIdentityConfig entraIdentityConfig = new EntraIdentityConfig(null, TENANT_ID, new InlinePassword(CLIENT_ID), new InlinePassword(CLIENT_SECRET), null, null);
        assertThat(entraIdentityConfig.getOauthEndpointOrDefault()).isEqualTo(URI.create("https://login.microsoftonline.com"));
        assertThat(entraIdentityConfig.getAuthScope()).isEqualTo(URI.create("https://vault.azure.net/.default"));
        assertThat(entraIdentityConfig.clientId().getProvidedPassword()).isEqualTo(CLIENT_ID);
        assertThat(entraIdentityConfig.clientSecret().getProvidedPassword()).isEqualTo(CLIENT_SECRET);
        assertThat(entraIdentityConfig.tenantId()).isEqualTo(TENANT_ID);
    }

    @Test
    void overrideOauthEndpoint() {
        EntraIdentityConfig entraIdentityConfig = new EntraIdentityConfig(URI.create("http://override.com"), TENANT_ID, new InlinePassword(CLIENT_ID),
                new InlinePassword(CLIENT_SECRET), null, null);
        assertThat(entraIdentityConfig.getOauthEndpointOrDefault()).isEqualTo(URI.create("http://override.com"));
    }

    @Test
    void overrideScope() {
        EntraIdentityConfig entraIdentityConfig = new EntraIdentityConfig(null, TENANT_ID, new InlinePassword(CLIENT_ID), new InlinePassword(CLIENT_SECRET),
                URI.create("http://override.com/.default"), null);
        assertThat(entraIdentityConfig.getAuthScope()).isEqualTo(URI.create("http://override.com/.default"));
    }
}
