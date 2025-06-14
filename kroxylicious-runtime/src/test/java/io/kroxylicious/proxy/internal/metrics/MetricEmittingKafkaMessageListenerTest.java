/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;

import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.util.Metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricEmittingKafkaMessageListenerTest {

    @Mock
    private Frame message;
    @Mock
    private Counter counter;
    @Mock
    private Meter.MeterProvider<Counter> counterMeterProvider;

    @Mock
    private DistributionSummary size;
    @Mock
    private Meter.MeterProvider<DistributionSummary> sizeMeterProvider;

    @BeforeEach
    void beforeEach() {
        when(counterMeterProvider.withTags(any(String[].class))).thenReturn(counter);
        when(sizeMeterProvider.withTags(any(String[].class))).thenReturn(size);
    }

    @Test
    void shouldTickBoth() {
        // Given
        when(message.apiKeyId()).thenReturn(ApiKeys.PRODUCE.id);

        var listener = new MetricEmittingKafkaMessageListener(counterMeterProvider, sizeMeterProvider);

        // When
        listener.onMessage(message, 1024);

        // Then
        verify(counterMeterProvider).withTags(Metrics.DECODED_LABEL, "false",
                Metrics.API_KEY_LABEL, ApiKeys.PRODUCE.name(),
                Metrics.API_VERSION_LABEL, "0");
        verify(sizeMeterProvider).withTags(Metrics.DECODED_LABEL, "false",
                Metrics.API_KEY_LABEL, ApiKeys.PRODUCE.name(),
                Metrics.API_VERSION_LABEL, "0");
        verify(counter).increment();
        verify(size).record(1024.0);
    }

}