/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BannerLoggerTest {

    private BannerLogger bannerLogger;
    @Mock
    private LoggingEventBuilder loggingEventBuilder;

    private Stream<String> bannerStream = Stream.empty();

    @BeforeEach
    void setUp() {
        Logger testLogger = mock(Logger.class);
        when(testLogger.atLevel(any())).thenReturn(loggingEventBuilder);
        bannerLogger = new BannerLogger(testLogger, () -> bannerStream);
    }

    @Test
    void shouldAppendToConfiguredLogger() {
        // Given
        bannerStream = Stream.of("banner_text");

        // When
        bannerLogger.log();

        // Then
        verify(loggingEventBuilder).log(anyString());
    }

    @Test
    void shouldLogSingleLineBanner() {
        // Given
        bannerStream = Stream.of("banner_text");

        // When
        bannerLogger.log();

        // Then
        verify(loggingEventBuilder).log("banner_text");
    }
}
