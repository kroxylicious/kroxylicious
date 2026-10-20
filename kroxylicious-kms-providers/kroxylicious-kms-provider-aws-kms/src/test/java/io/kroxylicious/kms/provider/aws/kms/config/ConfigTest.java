/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

import io.github.sambarker.logsquelcher.CapturedLogs;
import io.github.sambarker.logsquelcher.LogSquelcherExtension;
import io.github.sambarker.logsquelcher.LoggingEventAssert;

@ExtendWith(LogSquelcherExtension.class)
class ConfigTest {

    @Test
    void shouldWarnWithHTTPEndpoint(final CapturedLogs logs) {
        // Given
        final URI uri = URI.create("http://kms.us-east-1.amazonaws.com");
        var credentials = new CredentialsConfig(null, null, null, null);

        // When
        new Config(uri, credentials, "SomeRegion", null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .anyMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }

    @Test
    void shouldNotWarnWithHTTPSEndpoint(final CapturedLogs logs) {
        // Given
        final URI uri = URI.create("https://kms.us-east-1.amazonaws.com");
        var credentials = new CredentialsConfig(null, null, null, null);

        // When
        new Config(uri, credentials, "SomeRegion", null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .noneMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }
}
