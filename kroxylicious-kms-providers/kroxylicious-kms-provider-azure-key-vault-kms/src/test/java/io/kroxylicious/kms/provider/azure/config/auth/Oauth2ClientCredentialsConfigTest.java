/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

import io.github.sambarker.logsquelcher.CapturedLogs;
import io.github.sambarker.logsquelcher.LogSquelcherExtension;
import io.github.sambarker.logsquelcher.LoggingEventAssert;

import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(LogSquelcherExtension.class)
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

    @Test
    void shouldWarnWithHTTPEndpoint(final CapturedLogs logs) {
        //Given
        final URI uri = URI.create("http://login.microsoftonline.com");

        //When
        new Oauth2ClientCredentialsConfig(uri, TENANT_ID,
                new InlinePassword(CLIENT_ID),
                new InlinePassword(CLIENT_SECRET),
                URI.create("http://override.com/.default"), null);

        //Then
        final List<LoggingEvent> events = logs.logged(Oauth2ClientCredentialsConfig.class);
        LoggingEventAssert.assertThat(events)
                .anyMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("OAuth endpoint does not begin with https"));
    }

    @Test
    void shouldNotWarnWithHTTPSEndpoint(final CapturedLogs logs) {
        //Given
        final URI uri = URI.create("https://login.microsoftonline.com");

        //When
        new Oauth2ClientCredentialsConfig(uri, TENANT_ID,
                new InlinePassword(CLIENT_ID),
                new InlinePassword(CLIENT_SECRET),
                URI.create("http://override.com/.default"), null);

        //Then
        final List<LoggingEvent> events = logs.logged(Oauth2ClientCredentialsConfig.class);
        LoggingEventAssert.assertThat(events)
                .noneMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("OAuth endpoint does not begin with https"));
    }
}
