/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import javax.net.ssl.SSLHandshakeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static java.util.Objects.requireNonNull;

public class KafkaProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyBackendHandler.class);

    private final KafkaProxyFrontendHandler frontendHandler;
    private final ChannelHandlerContext inboundCtx;
    private ChannelHandlerContext blockedOutboundCtx;
    private boolean unflushedWrites;

    public KafkaProxyBackendHandler(KafkaProxyFrontendHandler frontendHandler, ChannelHandlerContext inboundCtx) {
        this.frontendHandler = frontendHandler;
        this.inboundCtx = requireNonNull(inboundCtx);
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        frontendHandler.outboundWritabilityChanged(ctx);
    }

    public void inboundChannelWritabilityChanged(ChannelHandlerContext inboundCtx) {
        assert inboundCtx == this.inboundCtx;
        final ChannelHandlerContext outboundCtx = blockedOutboundCtx;
        if (outboundCtx != null && inboundCtx.channel().isWritable()) {
            blockedOutboundCtx = null;
            outboundCtx.channel().config().setAutoRead(true);
        }
    }

    // Called when the outbound channel is active
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.trace("Channel active {}", ctx);
        this.frontendHandler.onUpstreamChannelActive(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        assert blockedOutboundCtx == null;
        LOGGER.trace("Channel read {}", msg);
        final Channel inboundChannel = inboundCtx.channel();
        if (inboundChannel.isWritable()) {
            inboundChannel.write(msg, inboundCtx.voidPromise());
            unflushedWrites = true;
        }
        else {
            inboundChannel.writeAndFlush(msg, inboundCtx.voidPromise());
            unflushedWrites = false;
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        final Channel inboundChannel = inboundCtx.channel();
        if (unflushedWrites) {
            unflushedWrites = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            ctx.channel().config().setAutoRead(false);
            this.blockedOutboundCtx = ctx;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        frontendHandler.closeInboundWithNoResponse();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // If the frontEnd has an exception handler for this exception
        // it's likely to have already dealt with it.
        // So only act here if its un-expected by the front end.
        try {
            throw cause;
        }
        catch (SSLHandshakeException e) {
            // frontendHandler.onSslHandshakeException(e);
        }
        catch (Throwable t) {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .addArgument(cause != null ? cause.getMessage() : "")
                    .log("Netty caught exception from the backend: {}. Increase log level to DEBUG for stacktrace");
            // TODO why are we asking the frontendHander to close
            // but passing it the backend channel???
        }
        finally {
            frontendHandler.closeInboundWithNoResponse();
        }
    }
}
