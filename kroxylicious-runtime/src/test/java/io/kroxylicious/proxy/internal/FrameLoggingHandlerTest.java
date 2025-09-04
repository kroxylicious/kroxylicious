/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FrameLoggingHandlerTest {

    public static final String FRAME_LOGGER = "FrameLogger";

    @Mock
    InternalLogger nettyLogger;

    private EmbeddedChannel embeddedChannel;

    @BeforeEach
    void setUp() {
        FrameLoggingHandler frameLogger = new FrameLoggingHandler(FRAME_LOGGER, LogLevel.INFO, nettyLogger);
        embeddedChannel = new EmbeddedChannel(frameLogger);
    }

    @Test
    void shouldConstructInstance() {
        // Given

        // When
        FrameLoggingHandler frameLoggingHandler = new FrameLoggingHandler(FRAME_LOGGER, LogLevel.INFO);

        // Then
        assertThat(frameLoggingHandler)
                .isNotNull()
                .hasFieldOrPropertyWithValue("frameLevel", InternalLogLevel.INFO);
        // Note the change of type we construct with a LogLevel but assert a InternalLogLevel
        // I have no clue why they have two different enums.
    }

    @Test
    void shouldNotLogReadIfLevelDisabled() {
        // Given
        when(nettyLogger.isEnabled(InternalLogLevel.INFO)).thenReturn(false);

        // When
        embeddedChannel.writeInbound("TestMessage");

        // Then
        verify(nettyLogger).isEnabled(InternalLogLevel.INFO);
        verifyNoMoreInteractions(nettyLogger);
    }

    @Test
    void shouldLogReadIfLevelEnabled() {
        // Given
        when(nettyLogger.isEnabled(InternalLogLevel.INFO)).thenReturn(true);

        // When
        embeddedChannel.writeInbound("TestMessage");

        // Then
        verify(nettyLogger).log(eq(InternalLogLevel.INFO), contains("READ"));
    }

    @Test
    void shouldNotLogWriteIfLevelDisabled() {
        // Given
        when(nettyLogger.isEnabled(InternalLogLevel.INFO)).thenReturn(false);

        // When
        embeddedChannel.writeOutbound("TestMessage");

        // Then
        verify(nettyLogger).isEnabled(InternalLogLevel.INFO);
        verifyNoMoreInteractions(nettyLogger);
    }

    @Test
    void shouldLogWriteIfLevelEnabled() {
        // Given
        when(nettyLogger.isEnabled(InternalLogLevel.INFO)).thenReturn(true);

        // When
        embeddedChannel.writeOutbound("TestMessage");

        // Then
        verify(nettyLogger).log(eq(InternalLogLevel.INFO), contains("WRITE"));
    }
}
