/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.config;

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
        final URI uri = URI.create("http://api.uk.smartkey.io");

        // When
        new Config(uri, null, null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .anyMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }

    @Test
    void shouldNotWarnWithHTTPSEndpoint(final CapturedLogs logs) {
        // given
        final URI uri = URI.create("https://api.uk.smartkey.io");

        // When
        new Config(uri, null, null);

        // Then
        final List<LoggingEvent> events = logs.logged(Config.class);
        LoggingEventAssert.assertThat(events)
                .noneMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("KMS endpoint does not begin with https"));
    }
}
