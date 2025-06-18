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

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UpstreamMessageCountingKafkaMessageListenerTest {
    @Mock
    private OpaqueResponseFrame opaqueRequest;

    @Mock
    private DecodedResponseFrame<?> decodedRequest;

    @Mock
    private DistributionSummary size;

    @Mock
    private Meter.MeterProvider<DistributionSummary> sizeMeterProvider;

    @Test
    @SuppressWarnings("removal")
    void opaqueFrameIsIgnored() {
        // Given
        var listener = new UpstreamPayloadSizeMetricRecordingKafkaMessageListener(sizeMeterProvider);

        // When
        listener.onMessage(opaqueRequest, 1024);

        // Then
        verifyNoInteractions(size);
    }

    @Test
    @SuppressWarnings("removal")
    void decodedFrameIncrementsAll() {
        // Given
        when(sizeMeterProvider.withTags(any(String[].class))).thenReturn(size);
        when(decodedRequest.apiKey()).thenReturn(ApiKeys.PRODUCE);

        var listener = new UpstreamPayloadSizeMetricRecordingKafkaMessageListener(sizeMeterProvider);

        // When
        listener.onMessage(decodedRequest, 1024);

        // Then
        verify(size).record(1024 - Frame.FRAME_SIZE_LENGTH);
    }

}
