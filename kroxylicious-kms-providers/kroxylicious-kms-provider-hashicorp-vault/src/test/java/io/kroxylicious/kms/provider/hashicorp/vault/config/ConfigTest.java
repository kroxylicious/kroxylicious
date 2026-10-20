/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

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

@ExtendWith(LogSquelcherExtension.class)
class ConfigTest {
    @Test
    void shouldWarnWithTransitEngineHTTPEndpoint(final CapturedLogs logs) {
        // Given
        final URI uri = URI.create("http://vault/v1/transit");

        // When
        new Config(null, null, null, uri, new InlinePassword(""), null, null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .anyMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }

    @Test
    void shouldNotWarnWithTransitEngineHTTPSEndpoint(final CapturedLogs logs) {
        // Given
        final URI uri = URI.create("https://vault/v1/transit");

        // When
        new Config(null, null, null, uri, new InlinePassword(""), null, null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .noneMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }

    @Test
    void shouldWarnWithVaultUrlHTTPEndpoint(final CapturedLogs logs) {
        // Given
        final URI uri = URI.create("http://vault/v1");

        // When
        new Config(uri, null, null, null, null, new VaultCredentialsConfig(new TokenCredentialsConfig(new InlinePassword(""))), null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .anyMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }

    @Test
    void shouldNotWarnWithVaultUrlHTTPSEndpoint(final CapturedLogs logs) {
        // Given
        final URI uri = URI.create("https://vault/v1");

        // when
        new Config(uri, null, null, null, null, new VaultCredentialsConfig(new TokenCredentialsConfig(new InlinePassword(""))), null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .noneMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }
}
