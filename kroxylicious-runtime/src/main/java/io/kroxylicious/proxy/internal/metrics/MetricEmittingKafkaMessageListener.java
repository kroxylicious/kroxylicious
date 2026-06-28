/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;

import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.util.Metrics;

/**
 * A kafka message listener that emits message count and message size
 * metrics.
 */
public class MetricEmittingKafkaMessageListener implements KafkaMessageListener {

    private final Meter.MeterProvider<Counter> messageCounterProvider;
    private final Meter.MeterProvider<DistributionSummary> messageSizeProvider;

    public MetricEmittingKafkaMessageListener(Meter.MeterProvider<Counter> messageCounterProvider,
                                              Meter.MeterProvider<DistributionSummary> messageSizeProvider) {
        this.messageCounterProvider = Objects.requireNonNull(messageCounterProvider);
        this.messageSizeProvider = Objects.requireNonNull(messageSizeProvider);
    }

    @Override
    public void onMessage(Frame frame, int wireLength) {
        var apiKey = ApiKeys.forId(frame.apiKeyId());
        short version = frame.apiVersion();
        boolean decoded = frame.isDecoded();
        messageCounterProvider
                .withTags(Metrics.DECODED_LABEL, Boolean.toString(decoded),
                        Metrics.API_KEY_LABEL, apiKey.name(),
                        Metrics.API_VERSION_LABEL, Short.toString(version))
                .increment();

        messageSizeProvider
                .withTags(Metrics.DECODED_LABEL, Boolean.toString(decoded),
                        Metrics.API_KEY_LABEL, apiKey.name(),
                        Metrics.API_VERSION_LABEL, Short.toString(version))
                .record(wireLength);

    }
}
