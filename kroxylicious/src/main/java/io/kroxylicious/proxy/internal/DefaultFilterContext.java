/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.future.InternalCompletionStage;
import io.kroxylicious.proxy.internal.util.ByteBufOutputStream;

/**
 * Implementation of {@link KrpcFilterContext}.
 */
class DefaultFilterContext implements KrpcFilterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFilterContext.class);
    public static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private final DecodedFrame<?, ?> decodedFrame;
    private final ChannelHandlerContext channelContext;
    private final ChannelPromise promise;
    private final KrpcFilter filter;
    private final long timeoutMs;
    private final String sniHostname;
    private CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    DefaultFilterContext(KrpcFilter filter,
                         ChannelHandlerContext channelContext,
                         DecodedFrame<?, ?> decodedFrame,
                         ChannelPromise promise,
                         long timeoutMs,
                         String sniHostname) {
        this.filter = filter;
        this.channelContext = channelContext;
        this.decodedFrame = decodedFrame;
        this.promise = promise;
        this.timeoutMs = timeoutMs;
        this.sniHostname = sniHostname;
    }

    /**
     * Get a description of the channel, typically used for logging.
     * @return a description of the channel.
     */
    @Override
    public String channelDescriptor() {
        return channelContext.channel().toString();
    }

    /**
     * Create a ByteBufferOutputStream of the given capacity.
     * The backing buffer will be deallocated when the request processing is completed
     * @param initialCapacity The initial capacity of the buffer.
     * @return The allocated ByteBufferOutputStream
     */
    @Override
    public ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity) {
        final ByteBuf buffer = channelContext.alloc().ioBuffer(initialCapacity);
        decodedFrame.add(buffer);
        return new ByteBufOutputStream(buffer);
    }

    @Override
    public String sniHostname() {
        return sniHostname;
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the upstream broker).
     *
     * @param header The header
     * @param message The message
     */
    @Override
    public void forwardRequest(RequestHeaderData header, ApiMessage message) {
        if (decodedFrame.body() != message) {
            IllegalStateException illegalStateException = new IllegalStateException();
            closeFuture.completeExceptionally(illegalStateException);
            throw illegalStateException;
        }
        if (decodedFrame.header() != header) {
            IllegalStateException illegalStateException = new IllegalStateException();
            closeFuture.completeExceptionally(illegalStateException);
            throw illegalStateException;
        }
        // check it's a request
        String name = message.getClass().getName();
        if (!name.endsWith("RequestData")) {
            AssertionError error = new AssertionError("Attempt to use forwardRequest with a non-request: " + name);
            closeFuture.completeExceptionally(error);
            throw error;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Forwarding request: {}", channelDescriptor(), decodedFrame);
        }
        // TODO check we've not forwarded it already
        if (!channelContext.executor().inEventLoop()) {
            channelContext.executor().execute(() -> {
                try {
                    channelContext.writeAndFlush(decodedFrame, promise);
                    closeFuture.complete(null);
                }
                catch (Exception e) {
                    closeFuture.completeExceptionally(e);
                }
            });
        }
        else {
            channelContext.write(decodedFrame, promise);
            closeFuture.complete(null);
        }
    }

    @Override
    public <T extends ApiMessage> CompletionStage<T> sendRequest(short apiVersion, ApiMessage message) {
        short key = message.apiKey();
        var apiKey = ApiKeys.forId(key);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        var header = new RequestHeaderData()
                .setCorrelationId(-1)
                .setRequestApiKey(key)
                .setRequestApiVersion(apiVersion);
        if (headerVersion > 1) {
            header.setClientId(filter.getClass().getSimpleName() + "@" + System.identityHashCode(filter));
        }
        boolean hasResponse = apiKey != ApiKeys.PRODUCE
                || ((ProduceRequestData) message).acks() != 0;
        var filterPromise = new CompletableFuture<T>();
        var filterStage = new InternalCompletionStage<>(filterPromise);
        var frame = new InternalRequestFrame<>(
                apiVersion, -1, hasResponse,
                filter, filterPromise, header, message);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Sending request: {}", channelDescriptor(), frame);
        }
        ChannelPromise writePromise = channelContext.channel().newPromise();
        // if (outboundCtx.channel().isWritable()) {
        // outboundCtx.write(frame, writePromise);
        // }
        // else {
        channelContext.writeAndFlush(frame, writePromise);
        // }

        if (!hasResponse) {
            // Complete the filter promise for an ack-less Produce
            // based on the success of the channel write
            // (for all other requests the filter promise will be completed
            // when handling the response).
            writePromise.addListener(f -> {
                if (f.isSuccess()) {
                    filterPromise.complete(null);
                }
                else {
                    filterPromise.completeExceptionally(f.cause());
                }
            });
        }

        channelContext.executor().schedule(() -> {
            LOGGER.debug("{}: Timing out {} request after {}ms", channelContext, apiKey, timeoutMs);
            filterPromise.completeExceptionally(new TimeoutException());
        }, timeoutMs, TimeUnit.MILLISECONDS);
        return filterStage;
    }

    /**
     * Forward a request to the next filter in the chain
     * (or to the downstream client).
     *
     * @param header   The header
     * @param response The message
     */
    @Override
    public void forwardResponse(ResponseHeaderData header, ApiMessage response) {
        // check it's a response
        CompletionStage<Void> future = forwardResponseAsync(header, response);
        future.whenComplete((unused, throwable) -> {
            if (throwable != null) {
                closeFuture.completeExceptionally(throwable);
            }
            else {
                closeFuture.complete(null);
            }
        });
    }

    private CompletionStage<Void> forwardResponseAsync(ResponseHeaderData header, ApiMessage response) {
        String name = response.getClass().getName();
        if (!name.endsWith("ResponseData")) {
            return CompletableFuture.failedFuture(new AssertionError("Attempt to use forwardResponse with a non-response: " + name));
        }
        if (decodedFrame instanceof RequestFrame) {
            return forwardShortCircuitResponse(header, response);
        }
        else {
            // TODO check we've not forwarded it already
            if (decodedFrame.body() != response) {
                return CompletableFuture.failedFuture(new AssertionError());
            }
            if (decodedFrame.header() != header) {
                return CompletableFuture.failedFuture(new AssertionError());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Forwarding response: {}", channelDescriptor(), decodedFrame);
            }
            if (!channelContext.executor().inEventLoop()) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                channelContext.executor().execute(() -> {
                    try {
                        channelContext.fireChannelRead(decodedFrame);
                        channelContext.fireChannelReadComplete();
                        future.complete(null);
                    }
                    catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
                return future;
            }
            else {
                channelContext.fireChannelRead(decodedFrame);
                return COMPLETED_FUTURE;
            }
        }
    }

    @Override
    public void forwardResponse(ApiMessage response) {
        if (decodedFrame instanceof RequestFrame) {
            this.forwardResponse(new ResponseHeaderData().setCorrelationId(decodedFrame.correlationId()), response);
        }
        else {
            this.forwardResponse((ResponseHeaderData) decodedFrame.header(), response);
        }
    }

    @Override
    public void closeConnection() {
        this.channelContext.close().addListener(future -> {
            LOGGER.debug("{} closed.", channelDescriptor());
            closeFuture.complete(null);
        });
    }

    public void discard() {
        closeFuture.complete(null);
    }

    /**
     * In this case we are not forwarding to the proxied broker but responding immediately.
     * We want to check that the ApiMessage is the correct type for the request. Ie if the
     * request was a Produce Request we want the response to be a Produce Response.
     *
     * @param header the response header
     * @param response the response body
     */
    private CompletionStage<Void> forwardShortCircuitResponse(ResponseHeaderData header, ApiMessage response) {
        if (response.apiKey() != decodedFrame.apiKey().id) {
            return CompletableFuture.failedFuture(new AssertionError(
                    "Attempt to respond with ApiMessage of type " + ApiKeys.forId(response.apiKey()) + " but request is of type " + decodedFrame.apiKey()));
        }
        DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(decodedFrame.apiVersion(), decodedFrame.correlationId(), header, response);
        decodedFrame.transferBuffersTo(responseFrame);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Forwarding response: {}", channelDescriptor(), decodedFrame);
        }
        if (!channelContext.executor().inEventLoop()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            channelContext.executor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        channelContext.fireChannelRead(responseFrame);
                        channelContext.fireChannelReadComplete();
                        future.complete(null);
                    }
                    catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            });
            return future;
        }
        else {
            // required to flush the message back to the client
            channelContext.fireChannelRead(responseFrame);
            channelContext.fireChannelReadComplete();
            return COMPLETED_FUTURE;
        }
    }

    public CompletableFuture<Void> onClose() {
        return closeFuture;
    }
}
