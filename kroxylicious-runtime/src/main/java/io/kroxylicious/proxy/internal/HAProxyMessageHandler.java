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

    private final KafkaSession kafkaSession;
    private final ProxyChannelStateMachine proxyChannelStateMachine;

    public HAProxyMessageHandler(KafkaSession kafkaSession, ProxyChannelStateMachine proxyChannelStateMachine) {
        this.kafkaSession = kafkaSession;
        this.proxyChannelStateMachine = proxyChannelStateMachine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Received HAProxy message: sourceAddress={}, sourcePort={}, destinationAddress={}, destinationPort={}",
                        ctx.channel(),
                        haProxyMessage.sourceAddress(),
                        haProxyMessage.sourcePort(),
                        haProxyMessage.destinationAddress(),
                        haProxyMessage.destinationPort());
            }
            // Store in the session so it is available to the state machine and consumers
            // (e.g. audit logging) regardless of when the message arrives relative to
            // SSL/binding resolution.
            kafkaSession.onHaProxyMessage(haProxyMessage);
            // Signal the state machine. If the frontend handler is not yet active (TLS
            // pipeline: PROXY header arrives before the SSL handshake), the state machine
            // defers processing until onClientActive() fires (see ProxyChannelStateMachine).
            proxyChannelStateMachine.onClientRequest(haProxyMessage);
        }
        else {
            // Pass all other messages (Kafka frames) to the next handler
            ctx.fireChannelRead(msg);
        }
    }
}
