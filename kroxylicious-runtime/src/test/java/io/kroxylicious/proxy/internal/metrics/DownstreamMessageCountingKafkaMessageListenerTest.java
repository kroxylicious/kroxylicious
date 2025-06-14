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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DownstreamMessageCountingKafkaMessageListenerTest {
    @Mock
    private OpaqueRequestFrame opaqueRequest;

    @Mock
    private DecodedRequestFrame<?> decodedRequest;
    @Mock
    private Counter inboundMessageCounter;

    @Mock
    private Counter decodedMessageCounter;
    @Mock
    private DistributionSummary size;

    @Mock
    private Meter.MeterProvider<DistributionSummary> sizeMeterProvider;

    @Test
    @SuppressWarnings("removal")
    void opaqueFrameIncrementsOnlyInboundMessageCounter() {
        // Given
        var listener = new DownstreamMessageCountingKafkaMessageListener(inboundMessageCounter, decodedMessageCounter, sizeMeterProvider);

        // When
        listener.onMessage(opaqueRequest, 1024);

        // Then
        verify(inboundMessageCounter).increment();
        verifyNoInteractions(decodedMessageCounter, size);
    }

    @Test
    @SuppressWarnings("removal")
    void decodedFrameIncrementsAll() {
        // Given
        when(sizeMeterProvider.withTags(any(String[].class))).thenReturn(size);
        when(decodedRequest.apiKey()).thenReturn(ApiKeys.PRODUCE);

        var listener = new DownstreamMessageCountingKafkaMessageListener(inboundMessageCounter, decodedMessageCounter, sizeMeterProvider);

        // When
        listener.onMessage(decodedRequest, 1024);

        // Then
        verify(inboundMessageCounter).increment();
        verify(decodedMessageCounter).increment();
        verify(size).record(1024 - Frame.FRAME_SIZE_LENGTH);
    }

}