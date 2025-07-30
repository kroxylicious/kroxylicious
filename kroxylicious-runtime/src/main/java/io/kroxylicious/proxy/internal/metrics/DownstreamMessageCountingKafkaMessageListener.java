/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter.MeterProvider;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.util.Metrics;

/**
 * Emits the deprecated downstream metrics kroxylicious_inbound_downstream_messages,
 * kroxylicious_inbound_downstream_messages,  kroxylicious_inbound_downstream_decoded_messages, and
 * kroxylicious_payload_size_bytes.
 *
 * @deprecated use metrics emitted by {@link MetricEmittingKafkaMessageListener} instead.
 */
@Deprecated(since = "0.13.0", forRemoval = true)
public class DownstreamMessageCountingKafkaMessageListener implements KafkaMessageListener {
    private final Counter messageCounter;
    private final Counter decodedMessagesCounter;
    private final MeterProvider<DistributionSummary> messageSizeDistributionProvider;

    public DownstreamMessageCountingKafkaMessageListener(Counter messageCounter,
                                                         Counter decodedMessageCounter,
                                                         MeterProvider<DistributionSummary> decodedMessageSizeProvider) {
        this.messageCounter = Objects.requireNonNull(messageCounter);
        this.decodedMessagesCounter = Objects.requireNonNull(decodedMessageCounter);
        this.messageSizeDistributionProvider = Objects.requireNonNull(decodedMessageSizeProvider);
    }

    @Override
    public void onMessage(Frame frame, int wireLength) {
        messageCounter.increment();
        if (frame instanceof DecodedRequestFrame<?> decodedRequestFrame) {
            decodedMessagesCounter.increment();
            var size = wireLength - Frame.FRAME_SIZE_LENGTH;
            var apiKey = decodedRequestFrame.apiKey();
            var apiVersion = decodedRequestFrame.apiVersion();
            messageSizeDistributionProvider
                    .withTags(Metrics.DEPRECATED_API_KEY_TAG, apiKey.name(),
                            Metrics.DEPRECATED_API_VERSION_TAG, String.valueOf(apiVersion))
                    .record(size);
        }
    }

}
