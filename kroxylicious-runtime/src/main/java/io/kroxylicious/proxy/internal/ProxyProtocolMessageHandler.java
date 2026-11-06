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
import io.netty.handler.codec.haproxy.HAProxyProtocolException;

import io.kroxylicious.proxy.internal.net.ProxyProtocolContext;

import static io.kroxylicious.proxy.internal.util.NettyFutures.logFailure;

/**
 * A channel handler that intercepts {@link HAProxyMessage} objects emitted by
 * Netty's {@link io.netty.handler.codec.haproxy.HAProxyMessageDecoder} and
 * stores the extracted context in the {@link KafkaSession}.
 * <p>
 * Any non-{@link HAProxyMessage} message (e.g. a TLS ClientHello that arrived in the same
 * TCP segment as the PROXY header) is forwarded downstream unchanged. This allows
 * {@link io.netty.handler.ssl.SniHandler} or the Kafka codec to receive those bytes
 * without them passing through the decoder a second time.
 * </p>
 * <p>
 * This handler removes itself from the pipeline after processing the PROXY message.
 * </p>
 * <p>
 * Named {@code ProxyProtocol*} rather than {@code HAProxy*} because "HA" is vendor branding
 * (HAProxy software); the protocol itself is called the PROXY Protocol.
 * </p>
 */
public class ProxyProtocolMessageHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyProtocolMessageHandler.class);

    private final KafkaSession kafkaSession;

    /**
     * Creates a handler that stores decoded PROXY-protocol messages in the given session.
     * @param kafkaSession the session in which the extracted {@link ProxyProtocolContext} is stored
     */
    public ProxyProtocolMessageHandler(KafkaSession kafkaSession) {
        this.kafkaSession = kafkaSession;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            try {
                LOGGER.atDebug()
                        .addKeyValue("sessionId", kafkaSession.sessionId())
                        .addKeyValue("sourceAddress", haProxyMessage.sourceAddress())
                        .addKeyValue("sourcePort", haProxyMessage.sourcePort())
                        .addKeyValue("destinationAddress", haProxyMessage.destinationAddress())
                        .addKeyValue("destinationPort", haProxyMessage.destinationPort())
                        .log("Received PROXY protocol message");
                kafkaSession.setProxyProtocolContext(ProxyProtocolContext.from(haProxyMessage));
            }
            finally {
                haProxyMessage.release();
                ctx.pipeline().remove(this);
            }
        }
        else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof HAProxyProtocolException) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .log("Connection rejected: PROXY protocol header required but client sent non-PROXY data. "
                            + "Ensure the upstream load balancer is configured to send PROXY protocol headers.");
            ctx.close().addListener(logFailure(LOGGER, "close after missing PROXY protocol header"));
        }
        else {
            ctx.fireExceptionCaught(cause);
        }
    }
}
