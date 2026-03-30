/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;

/**
 * A channel handler that intercepts {@link HAProxyMessage} objects emitted by
 * Netty's {@link io.netty.handler.codec.haproxy.HAProxyMessageDecoder} and
 * forwards them to the {@link ProxyChannelStateMachine} for processing.
 * <p>
 * This handler prevents {@link HAProxyMessage} from propagating further down
 * the pipeline to handlers (like {@link FilterHandler}) that only expect
 * Kafka protocol messages.
 * </p>
 * <p>
 * All other messages are passed through unchanged to the next handler in the pipeline.
 * </p>
 */
public class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HAProxyMessageHandler.class);

    private final ProxyChannelStateMachine proxyChannelStateMachine;

    public HAProxyMessageHandler(ProxyChannelStateMachine proxyChannelStateMachine) {
        this.proxyChannelStateMachine = proxyChannelStateMachine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            LOGGER.atDebug()
                    .addKeyValue("channelId", () -> ctx.channel().toString())
                    .addKeyValue("sourceAddress", haProxyMessage.sourceAddress())
                    .addKeyValue("sourcePort", haProxyMessage.sourcePort())
                    .addKeyValue("destinationAddress", haProxyMessage.destinationAddress())
                    .addKeyValue("destinationPort", haProxyMessage.destinationPort())
                    .log("received HAProxy message");
            // Forward to state machine for processing - do not propagate to filters
            proxyChannelStateMachine.onClientRequest(haProxyMessage);
        }
        else {
            // Pass all other messages (Kafka frames) to the next handler
            ctx.fireChannelRead(msg);
        }
    }
}
