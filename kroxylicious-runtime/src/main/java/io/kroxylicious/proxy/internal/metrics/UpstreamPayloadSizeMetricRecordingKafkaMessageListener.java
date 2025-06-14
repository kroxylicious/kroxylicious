/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.Objects;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter.MeterProvider;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.util.Metrics;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Emits the deprecated upstream metric kroxylicious_payload_size_bytes
 *
 * @deprecated use metrics emitted by {@link MetricEmittingKafkaMessageListener} instead.
 */
@Deprecated(since = "0.13.0", forRemoval = true)
public class UpstreamPayloadSizeMetricRecordingKafkaMessageListener implements KafkaMessageListener {
    private final MeterProvider<DistributionSummary> messageSizeDistributionProvider;

    public UpstreamPayloadSizeMetricRecordingKafkaMessageListener(MeterProvider<DistributionSummary> messageSizeDistributionProvider) {
        this.messageSizeDistributionProvider = Objects.requireNonNull(messageSizeDistributionProvider);
    }

    @Override
    @SuppressWarnings("removal")
    public void onMessage(@NonNull Frame frame, int wireLength) {
        // This metric records the size of decoded messages only.
        if (frame instanceof DecodedResponseFrame<?> decodedResponseFrame) {
            int size = wireLength - Frame.FRAME_SIZE_LENGTH;
            // There was a mistake in the original implementation with upstream/downstream switched about.
            // Maintain the error for compatibility's sake.
            messageSizeDistributionProvider
                    .withTags(Metrics.DEPRECATED_API_KEY_TAG, decodedResponseFrame.apiKey().name(),
                            Metrics.DEPRECATED_API_VERSION_TAG, String.valueOf(decodedResponseFrame.apiVersion()))
                    .record(size);
        }

    }
}
