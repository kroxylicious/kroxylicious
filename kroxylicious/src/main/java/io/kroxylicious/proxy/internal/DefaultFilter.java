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
import io.kroxylicious.proxy.filter.ReplacementResponseContext;
import io.kroxylicious.proxy.filter.RequestForwardingContext;
import io.kroxylicious.proxy.filter.ResponseForwardingContext;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.future.InternalCompletionStage;
import io.kroxylicious.proxy.internal.util.ByteBufOutputStream;

/**
 * Implementation of {@link KrpcFilterContext}.
 */
class DefaultFilter implements KrpcFilterContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFilter.class);
    public static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private final DecodedFrame<?, ?> decodedFrame;
    private final ChannelHandlerContext channelContext;
    private final ChannelPromise promise;
    private final KrpcFilter filter;
    private final long timeoutMs;
    private final String sniHostname;

    private State state = State.INIITIAL;

    public boolean isClosedOrDeferred() {
        return state != State.INIITIAL;
    }

    public boolean isDeferred() {
        return state == State.DEFERRED_REQUEST || state == State.DEFERRED_RESPONSE;
    }

    private enum State {
        INIITIAL,
        DEFERRED_REQUEST,
        DEFERRED_RESPONSE,
        COMPLETE
    }

    private CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    DefaultFilter(KrpcFilter filter,
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
        if (state == State.COMPLETE || state == State.DEFERRED_RESPONSE) {
            RuntimeException illegalStateException = new IllegalStateException("cannot forward request when in state: " + state);
            completeExceptionally(illegalStateException);
            throw illegalStateException;
        }
        if (!channelContext.executor().inEventLoop() && state != State.DEFERRED_REQUEST) {
            IllegalStateException illegalStateException = new IllegalStateException("We can only forward from another thread if we deferred the forward");
            completeExceptionally(illegalStateException);
            throw illegalStateException;
        }
        if (decodedFrame.body() != message) {
            IllegalStateException illegalStateException = new IllegalStateException();
            completeExceptionally(illegalStateException);
            throw illegalStateException;
        }
        if (decodedFrame.header() != header) {
            IllegalStateException illegalStateException = new IllegalStateException();
            completeExceptionally(illegalStateException);
            throw illegalStateException;
        }
        // check it's a request
        String name = message.getClass().getName();
        if (!name.endsWith("RequestData")) {
            AssertionError error = new AssertionError("Attempt to use forwardRequest with a non-request: " + name);
            completeExceptionally(error);
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
                    completeSuccessfully();
                }
                catch (Exception e) {
                    completeExceptionally(e);
                }
            });
        }
        else {
            channelContext.write(decodedFrame, promise);
            completeSuccessfully();
        }
    }

    private void completeExceptionally(Throwable throwable) {
        state = State.COMPLETE;
        completionFuture.completeExceptionally(throwable);
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

    @Override
    public <T extends ApiMessage> CompletionStage<ReplacementResponseContext<T>> replaceRequest(short apiVersion, ApiMessage request) {
        CompletionStage<ReplacementResponseContext<T>> future = sendRequest(apiVersion, request).thenApply(
                apiMessage -> new ReplacementResponseContext<T>() {
                    @Override
                    public ResponseForwardingContext context() {
                        return new DefaultFilter(filter, channelContext, decodedFrame, null, timeoutMs, sniHostname);
                    }

                    @Override
                    public T apiMessage() {
                        return (T) apiMessage;
                    }
                });
        completeSuccessfully();
        return future;
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
        if (state == State.COMPLETE || state == State.DEFERRED_REQUEST) {
            IllegalStateException exception = new IllegalStateException("cannot forward response when in state: " + state);
            completeExceptionally(exception);
            throw exception;
        }
        if (!channelContext.executor().inEventLoop() && state != State.DEFERRED_RESPONSE) {
            IllegalArgumentException exception = new IllegalArgumentException("We can only forward from another thread if we deferred the forward");
            completeExceptionally(exception);
            throw exception;
        }
        // check it's a response
        CompletionStage<Void> future = forwardResponseAsync(header, response);
        future.whenComplete((unused, throwable) -> {
            if (throwable != null) {
                completeExceptionally(throwable);
            }
            else {
                completeSuccessfully();
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
        if (state == State.COMPLETE || state == State.DEFERRED_REQUEST) {
            throw new IllegalStateException("cannot forward response when in state: " + state);
        }
        if (!channelContext.executor().inEventLoop() && state != State.DEFERRED_RESPONSE) {
            throw new IllegalArgumentException("We can only forward from another thread if we deferred the forward");
        }
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
        });
        completeSuccessfully();
    }

    public void discard() {
        if (state == State.COMPLETE) {
            throw new IllegalStateException("cannot discard message when in state: " + state);
        }
        completeSuccessfully();
    }

    private void completeSuccessfully() {
        state = State.COMPLETE;
        completionFuture.complete(null);
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
        return completionFuture;
    }

    @Override
    public RequestForwardingContext deferredForwardRequest() {
        if (!(decodedFrame instanceof RequestFrame)) {
            AssertionError error = new AssertionError("Attempt to defer forwardRequest but frame is not a RequestFrame");
            completeExceptionally(error);
            throw error;
        }
        this.state = State.DEFERRED_REQUEST;
        channelContext.executor().schedule(() -> {
            LOGGER.error("{}: Timing out deferring request for {} with correlationId {} after {}ms", channelContext, decodedFrame.apiKey(), decodedFrame.correlationId(),
                    timeoutMs);
            completeExceptionally(new TimeoutException());
        }, timeoutMs, TimeUnit.MILLISECONDS);
        return this;
    }

    @Override
    public ResponseForwardingContext deferredForwardResponse() {
        if (!(decodedFrame instanceof ResponseFrame)) {
            AssertionError error = new AssertionError("Attempt to defer forwardResponse but frame is not a ResponseFrame");
            completeExceptionally(error);
            throw error;
        }
        this.state = State.DEFERRED_RESPONSE;
        channelContext.executor().schedule(() -> {
            LOGGER.error("{}: Timing out deferring response for {} with correlationId {} after {}ms", channelContext, decodedFrame.apiKey(), decodedFrame.correlationId(),
                    timeoutMs);
            completeExceptionally(new TimeoutException());
        }, timeoutMs, TimeUnit.MILLISECONDS);
        return this;
    }
}
