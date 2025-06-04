/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.util.Metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageMetricsTest {

    private EmbeddedChannel embeddedChannel;

    @Mock
    Frame message;
    @Mock
    private Meter.MeterProvider<Counter> provider;

    @Mock
    Counter counter;

    @Test
    void shouldTickMetricOnFrameRead() {
        // Given
        when(provider.withTags(any(String[].class))).thenReturn(counter);
        when(message.apiKeyId()).thenReturn(ApiKeys.PRODUCE.id);

        var messageMetrics = new MessageMetrics(provider, null);
        embeddedChannel = new EmbeddedChannel(messageMetrics);

        // When
        embeddedChannel.writeInbound(message);

        // Then
        verify(provider).withTags(Metrics.DECODED_LABEL, "false",
                Metrics.API_KEY_LABEL, ApiKeys.PRODUCE.name(),
                Metrics.API_VERSION_LABEL, "0");
        verify(counter).increment();
    }

    @Test
    void shouldNotTickOnNonFrameRead() {
        // Given
        var messageMetrics = new MessageMetrics(provider, null);
        embeddedChannel = new EmbeddedChannel(messageMetrics);

        // When
        embeddedChannel.writeInbound(new Object());

        // Then
        verifyNoInteractions(provider, counter);
    }

    @Test
    void shouldTickMetricOnFrameWrite() {
        // Given
        when(provider.withTags(any(String[].class))).thenReturn(counter);
        when(message.apiKeyId()).thenReturn(ApiKeys.PRODUCE.id);

        var messageMetrics = new MessageMetrics(null, provider);
        embeddedChannel = new EmbeddedChannel(messageMetrics);

        // When
        embeddedChannel.writeOutbound(message);

        // Then
        verify(provider).withTags(Metrics.DECODED_LABEL, "false",
                Metrics.API_KEY_LABEL, ApiKeys.PRODUCE.name(),
                Metrics.API_VERSION_LABEL, "0");
        verify(counter).increment();
    }

    @Test
    void shouldNotTickOnNonFrameWrite() {
        // Given
        var messageMetrics = new MessageMetrics(null, provider);
        embeddedChannel = new EmbeddedChannel(messageMetrics);

        // When
        embeddedChannel.writeOutbound(new Object());

        // Then
        verifyNoInteractions(provider, counter);
    }

}