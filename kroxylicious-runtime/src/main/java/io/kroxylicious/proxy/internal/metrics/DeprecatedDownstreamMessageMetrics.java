/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.util.Metrics;

/**
 * Emits the deprecated downstream metrics kroxylicious_inbound_downstream_messages,
 * kroxylicious_inbound_downstream_messages,  kroxylicious_inbound_downstream_decoded_messages, and
 * kroxylicious_payload_size_bytes.
 *
 * @deprecated use metrics emitted by {@link MessageMetrics} instead.
 */
@Deprecated(since = "0.13.0", forRemoval = true)
public class DeprecatedDownstreamMessageMetrics extends ChannelInboundHandlerAdapter {
    private final String clusterName;
    private final Counter inboundMessageCounter;
    private final Counter decodedMessagesCounter;

    @SuppressWarnings("removal")
    public DeprecatedDownstreamMessageMetrics(String clusterName) {
        this.clusterName = clusterName;
        List<Tag> tags = Metrics.tags(Metrics.FLOWING_TAG, Metrics.DOWNSTREAM, Metrics.VIRTUAL_CLUSTER_TAG, clusterName);
        inboundMessageCounter = Metrics.taggedCounter(Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES, tags);
        decodedMessagesCounter = Metrics.taggedCounter(Metrics.KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES, tags);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        try {
            inboundMessageCounter.increment();
            if (msg instanceof DecodedRequestFrame<?> decodedRequestFrame) {
                decodedMessagesCounter.increment();
                // The request might be updated by the filters as it travels through the proxy
                // so this avoids causing the size to be cached prematurely
                Metrics.payloadSizeBytesUpstreamSummary(decodedRequestFrame.apiKey(), decodedRequestFrame.apiVersion(), clusterName)
                        .record(decodedRequestFrame.originalEncodedSize());
            }
        }
        finally {
            super.channelRead(ctx, msg);
        }
    }
}
