/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.List;
import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.util.Metrics;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Emits the deprecated downstream metrics kroxylicious_inbound_downstream_messages,
 * kroxylicious_inbound_downstream_messages,  kroxylicious_inbound_downstream_decoded_messages, and
 * kroxylicious_payload_size_bytes.
 *
 * @deprecated use metrics emitted by {@link MetricEmittingKafkaMessageListener} instead.
 */
@Deprecated(since = "0.13.0", forRemoval = true)
public class DownstreamMessageCountingKafkaMessageListener implements KafkaMessageListener {
    private final String clusterName;
    private final Counter inboundMessageCounter;
    private final Counter decodedMessagesCounter;

    @SuppressWarnings("removal")
    public DownstreamMessageCountingKafkaMessageListener(@NonNull String clusterName) {
        this.clusterName = Objects.requireNonNull(clusterName);
        List<Tag> tags = Metrics.tags(Metrics.FLOWING_TAG, Metrics.DOWNSTREAM, Metrics.VIRTUAL_CLUSTER_TAG, clusterName);
        inboundMessageCounter = Metrics.taggedCounter(Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES, tags);
        decodedMessagesCounter = Metrics.taggedCounter(Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES, tags);
    }

    @Override
    public void onMessage(@NonNull Frame frame, int wireLength) {
        inboundMessageCounter.increment();
        if (frame instanceof DecodedRequestFrame<?> decodedRequestFrame) {
            decodedMessagesCounter.increment();
            int size = wireLength - Frame.FRAME_SIZE_LENGTH;
            Metrics.payloadSizeBytesUpstreamSummary(decodedRequestFrame.apiKey(), decodedRequestFrame.apiVersion(), clusterName)
                    .record(size);
        }
    }
}
