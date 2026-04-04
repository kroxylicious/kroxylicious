/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.haproxy.HAProxyMessage;

/**
 * A channel handler that intercepts {@link HAProxyMessage} objects emitted by
 * Netty's {@link io.netty.handler.codec.haproxy.HAProxyMessageDecoder} and
 * forwards them to the {@link ProxyChannelStateMachine} for processing.
 * <p>
 * Extends {@link SimpleChannelInboundHandler} so the reference-counted
 * {@link HAProxyMessage} is automatically released after
 * {@link #channelRead0(ChannelHandlerContext, HAProxyMessage) channelRead0} returns.
 * </p>
 * <p>
 * All other messages are passed through unchanged to the next handler in the pipeline.
 * </p>
 */
public class HaProxyMessageHandler extends SimpleChannelInboundHandler<HAProxyMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaProxyMessageHandler.class);

    private final ProxyChannelStateMachine proxyChannelStateMachine;

    public HaProxyMessageHandler(ProxyChannelStateMachine proxyChannelStateMachine) {
        super(HAProxyMessage.class, true);
        this.proxyChannelStateMachine = proxyChannelStateMachine;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HAProxyMessage haProxyMessage) {
        LOGGER.debug("{}: Received HaProxy message: sourceAddress={}, sourcePort={}, destinationAddress={}, destinationPort={}",
                ctx.channel(),
                haProxyMessage.sourceAddress(),
                haProxyMessage.sourcePort(),
                haProxyMessage.destinationAddress(),
                haProxyMessage.destinationPort());

        // Extract context into KafkaSession and signal the state machine.
        // The HaProxyContext deep-copies all fields, so it remains valid
        // after SimpleChannelInboundHandler releases the message.
        proxyChannelStateMachine.onHAProxyMessageReceived(haProxyMessage);

        // The PROXY header consumed a read() that was meant for the Kafka protocol layer.
        // Trigger another read so the next message (e.g. ApiVersionsRequest) is picked up.
        ctx.read();
    }
}
