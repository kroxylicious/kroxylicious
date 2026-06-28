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

class Oauth2ClientCredentialsConfigTest {

    public static final String CLIENT_ID = "abc";
    public static final String CLIENT_SECRET = "def";
    public static final String TENANT_ID = "tenant";
    public static final URI SCOPE = URI.create("https://vault.azure.net/.default");

    @Test
    void minimumAuthConfiguration() {
        Oauth2ClientCredentialsConfig oauth2ClientCredentials = new Oauth2ClientCredentialsConfig(URI.create("https://login.microsoftonline.com"), TENANT_ID,
                new InlinePassword(CLIENT_ID),
                new InlinePassword(CLIENT_SECRET), SCOPE, null);
        assertThat(oauth2ClientCredentials.oauthEndpoint()).isEqualTo(URI.create("https://login.microsoftonline.com"));
        assertThat(oauth2ClientCredentials.scope()).isEqualTo(SCOPE);
        assertThat(oauth2ClientCredentials.clientId().getProvidedPassword()).isEqualTo(CLIENT_ID);
        assertThat(oauth2ClientCredentials.clientSecret().getProvidedPassword()).isEqualTo(CLIENT_SECRET);
        assertThat(oauth2ClientCredentials.tenantId()).isEqualTo(TENANT_ID);
    }

    @Test
    void overrideOauthEndpoint() {
        Oauth2ClientCredentialsConfig oauth2ClientCredentials = new Oauth2ClientCredentialsConfig(URI.create("http://override.com"), TENANT_ID,
                new InlinePassword(CLIENT_ID),
                new InlinePassword(CLIENT_SECRET), SCOPE, null);
        assertThat(oauth2ClientCredentials.oauthEndpoint()).isEqualTo(URI.create("http://override.com"));
    }

    @Test
    void overrideScope() {
        Oauth2ClientCredentialsConfig oauth2ClientCredentials = new Oauth2ClientCredentialsConfig(URI.create("https://login.microsoftonline.com"), TENANT_ID,
                new InlinePassword(CLIENT_ID),
                new InlinePassword(CLIENT_SECRET),
                URI.create("http://override.com/.default"), null);
        assertThat(oauth2ClientCredentials.scope()).isEqualTo(URI.create("http://override.com/.default"));
    }
}
