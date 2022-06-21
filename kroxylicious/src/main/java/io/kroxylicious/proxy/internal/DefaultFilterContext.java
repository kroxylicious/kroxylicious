/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.future.ProxyFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Implementation of {@link KrpcFilterContext}.
 */
class DefaultFilterContext implements KrpcFilterContext, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(DefaultFilterContext.class);

    private final DecodedFrame<?, ?> decodedFrame;
    private final ChannelHandlerContext channelContext;
    private final ChannelPromise promise;
    private final KrpcFilter filter;
    private final ChannelHandlerContext outboundCtx;
    private boolean closed;

    DefaultFilterContext(KrpcFilter filter,
                         ChannelHandlerContext channelContext,
                         DecodedFrame<?, ?> decodedFrame,
                         ChannelPromise promise,
                         ChannelHandlerContext outboundCtx) {
        this.filter = filter;
        this.channelContext = channelContext;
        this.decodedFrame = decodedFrame;
        this.promise = promise;
        this.outboundCtx = outboundCtx;
        this.closed = false;
    }

    /**
     * Get a description of the channel, typically used for logging.
     * @return a description of the channel.
     */
    @Override
    public String channelDescriptor() {
        checkNotClosed();
        return channelContext.channel().toString();
    }

    /**
     * Allocate a buffer with the given {@code initialCapacity}.
     * The returned buffer will be released automatically
     * TODO when?
     * @param initialCapacity The initial capacity of the buffer.
     * @return The allocated buffer.
     */
    @Override
    public ByteBuf allocate(int initialCapacity) {
        checkNotClosed();
        final ByteBuf buffer = channelContext.alloc().heapBuffer(initialCapacity);
        decodedFrame.add(buffer);
        return buffer;
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the upstream broker).
     * @param message The message
     */
    @Override
    public void forwardRequest(ApiMessage message) {
        checkNotClosed();
        if (decodedFrame.body() != message) {
            throw new IllegalStateException();
        }
        // check it's a request
        String name = message.getClass().getName();
        if (!name.endsWith("RequestData")) {
            throw new AssertionError("Attempt to use forwardRequest with a non-request: " + name);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Forwarding request: {}", channelDescriptor(), decodedFrame);
        }
        // TODO check we've not forwarded it already
        channelContext.write(decodedFrame, promise);
    }

    @Override
    public <T extends ApiMessage> ProxyFuture<T> sendRequest(short apiVersion, ApiMessage message) {
        checkNotClosed();
        short key = message.apiKey();
        var apiKey = ApiKeys.forId(key);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        var header = new RequestHeaderData()
                .setCorrelationId(0)
                .setRequestApiKey(key)
                .setRequestApiVersion(apiVersion);
        if (headerVersion > 1) {
            header.setClientId(filter.getClass().getSimpleName() + "@" + System.identityHashCode(filter));
        }
        boolean hasResponse = apiKey != ApiKeys.PRODUCE
                || ((ProduceRequestData) message).acks() != 0;
        var filterPromise = new ProxyPromiseImpl<T>();
        var frame = DecodedRequestFrame.internalRequest(
                apiVersion, 0, hasResponse,
                filter, filterPromise, header, message);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Sending request: {}", channelDescriptor(), frame);
        }
        ChannelPromise writePromise = outboundCtx.voidPromise();
        // if (outboundCtx.channel().isWritable()) {
        // outboundCtx.write(frame, writePromise);
        // }
        // else {
        outboundCtx.writeAndFlush(frame, writePromise);
        // }

        if (!hasResponse) {
            writePromise.addListener(f -> {
                // Complete the filter promise for an ack-less Produce
                // based on the success of the channel write
                if (f.isSuccess()) {
                    filterPromise.complete(null);
                }
                else {
                    filterPromise.fail(f.cause());
                }
            });
        }
        // Otherwise, the filter promise will be completed when handling the response

        // TODO we probably need a timeout mechanism too
        return filterPromise;
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the downstream client).
     * @param response The message
     */
    @Override
    public void forwardResponse(ApiMessage response) {
        checkNotClosed();
        // check it's a response
        String name = response.getClass().getName();
        if (!name.endsWith("ResponseData")) {
            throw new AssertionError("Attempt to use forwardResponse with a non-response: " + name);
        }
        // TODO check we've not forwarded it already

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Forwarding response: {}", channelDescriptor(), decodedFrame);
        }
        channelContext.fireChannelRead(decodedFrame);
    }

    private void checkNotClosed() {
        // if (closed) {
        // throw new IllegalStateException("Context is closed");
        // }
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
