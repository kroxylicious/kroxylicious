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

import io.kroxylicious.proxy.internal.net.HaProxyContext;

/**
 * A channel handler that intercepts {@link HAProxyMessage} objects emitted by
 * Netty's {@link io.netty.handler.codec.haproxy.HAProxyMessageDecoder} and
 * stores the extracted context in the {@link KafkaSession}.
 * <p>
 * This handler prevents {@link HAProxyMessage} from propagating further down
 * the pipeline to handlers (like {@link FilterHandler}) that only expect
 * Kafka protocol messages. Extending {@link SimpleChannelInboundHandler} ensures
 * the ref-counted {@link HAProxyMessage} is released automatically after
 * {@link #channelRead0} returns; all other messages are forwarded unchanged.
 * </p>
 */
public class HaProxyMessageHandler extends SimpleChannelInboundHandler<HAProxyMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaProxyMessageHandler.class);

    private final KafkaSession kafkaSession;

    public HaProxyMessageHandler(KafkaSession kafkaSession) {
        this.kafkaSession = kafkaSession;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HAProxyMessage haProxyMessage) {
        LOGGER.atDebug()
                .addKeyValue("channelId", () -> ctx.channel().toString())
                .addKeyValue("sourceAddress", haProxyMessage.sourceAddress())
                .addKeyValue("sourcePort", haProxyMessage.sourcePort())
                .addKeyValue("destinationAddress", haProxyMessage.destinationAddress())
                .addKeyValue("destinationPort", haProxyMessage.destinationPort())
                .log("Received HAProxy message");
        kafkaSession.setHaProxyContext(HaProxyContext.from(haProxyMessage));
    }
}
