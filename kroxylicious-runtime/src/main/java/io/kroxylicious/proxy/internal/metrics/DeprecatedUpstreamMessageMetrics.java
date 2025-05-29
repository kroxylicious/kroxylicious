/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metrics;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.util.Metrics;

@Deprecated(since = "0.13.0", forRemoval = true)
public class DeprecatedUpstreamMessageMetrics extends ChannelInboundHandlerAdapter {
    private final String clusterName;

    @SuppressWarnings("removal")
    public DeprecatedUpstreamMessageMetrics(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        try {
            if (msg instanceof DecodedResponseFrame<?> decodedResponseFrame) {
                // TODO the request might be updated as it travels through the proxy
                // so this avoids caching the size prematurely
                int size = new DecodedResponseFrame<>(decodedResponseFrame.apiVersion(), decodedResponseFrame.correlationId(), decodedResponseFrame.header(),
                        decodedResponseFrame.body()).estimateEncodedSize();
                Metrics.payloadSizeBytesUpstreamSummary(decodedResponseFrame.apiKey(), decodedResponseFrame.apiVersion(), clusterName)
                        .record(size);
            }
        }
        finally {
            super.channelRead(ctx, msg);
        }
    }
}
