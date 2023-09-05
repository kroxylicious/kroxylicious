/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.ApiVersionsService;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterCommand;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.RequestFilterCommand;
import io.kroxylicious.proxy.filter.RequestFilterCommandBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterCommand;
import io.kroxylicious.proxy.filter.ResponseFilterCommandBuilder;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.future.InternalCompletionStage;
import io.kroxylicious.proxy.internal.filter.RequestFilterCommandBuilderImpl;
import io.kroxylicious.proxy.internal.filter.ResponseFilterCommandBuilderImpl;
import io.kroxylicious.proxy.internal.util.Assertions;
import io.kroxylicious.proxy.internal.util.ByteBufOutputStream;
import io.kroxylicious.proxy.model.VirtualCluster;

/**
 * A {@code ChannelInboundHandler} (for handling requests from downstream)
 * that applies a single {@link Filter}.
 */
public class FilterHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);
    private final Filter filter;
    private final FilterInvoker invoker;
    private final long timeoutMs;
    private final String sniHostname;
    private final VirtualCluster virtualCluster;
    private final Channel inboundChannel;
    private final ApiVersionsServiceImpl apiVersionService;
    private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> readFuture = CompletableFuture.completedFuture(null);
    private ChannelHandlerContext ctx;

    public FilterHandler(FilterAndInvoker filterAndInvoker, long timeoutMs, String sniHostname, VirtualCluster virtualCluster, Channel inboundChannel,
                         ApiVersionsServiceImpl apiVersionService) {
        this.filter = Objects.requireNonNull(filterAndInvoker).filter();
        this.invoker = filterAndInvoker.invoker();
        this.timeoutMs = Assertions.requireStrictlyPositive(timeoutMs, "timeout");
        this.sniHostname = sniHostname;
        this.virtualCluster = virtualCluster;
        this.inboundChannel = inboundChannel;
        this.apiVersionService = apiVersionService;
    }

    String filterDescriptor() {
        return filter.getClass().getSimpleName() + "@" + System.identityHashCode(filter);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof InternalResponseFrame<?> decodedFrame) {
            // jump the queue, let responses to asynchronous requests flow back to their sender
            if (decodedFrame.isRecipient(filter)) {
                completeInternalResponse(decodedFrame);
            }
            else {
                readDecodedResponse(decodedFrame);
            }
        }
        else if (msg instanceof DecodedResponseFrame<?> decodedFrame) {
            if (readFuture.isDone()) {
                readFuture = readDecodedResponse(decodedFrame);
            }
            else {
                readFuture = readFuture.thenCompose(ignored -> {
                    if (ctx.channel().isOpen()) {
                        return readDecodedResponse(decodedFrame);
                    }
                    else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).exceptionally(throwable -> null);
            }
        }
        else {
            if (!(msg instanceof OpaqueResponseFrame)) {
                throw new IllegalStateException("Unexpected message reading from upstream:  " + msg);
            }
            readFuture = readFuture.whenComplete((a, b) -> {
                if (ctx.channel().isOpen()) {
                    ctx.fireChannelRead(msg);
                }
            });
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof InternalRequestFrame<?> decodedFrame) {
            // jump the queue, internal request must flow!
            writeDecodedRequest(decodedFrame, promise);
        }
        else if (msg instanceof DecodedRequestFrame<?> decodedFrame) {
            if (writeFuture.isDone()) {
                writeFuture = writeDecodedRequest(decodedFrame, promise);
            }
            else {
                writeFuture = writeFuture.thenCompose(ignored -> {
                    if (ctx.channel().isOpen()) {
                        return writeDecodedRequest(decodedFrame, promise);
                    }
                    else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).exceptionally(throwable -> null);
            }
        }
        else {
            if (!(msg instanceof OpaqueRequestFrame) && msg != Unpooled.EMPTY_BUFFER) {
                // Unpooled.EMPTY_BUFFER is used by KafkaProxyFrontendHandler#closeOnFlush
                // but, otherwise we don't expect any other kind of message
                throw new IllegalStateException("Unexpected message writing to upstream: " + msg);
            }
            writeFuture.whenComplete((unused, throwable) -> {
                if (ctx.channel().isOpen()) {
                    ctx.write(msg, promise);
                }
            });
        }
    }

    private CompletableFuture<Void> readDecodedResponse(DecodedResponseFrame<?> decodedFrame) {
        var filterContext = new InternalFilterContext(decodedFrame, apiVersionService);

        final var future = dispatchDecodedResponseFrame(decodedFrame, filterContext);
        boolean defer = !future.isDone();
        if (defer) {
            return configureResponseFilterChain(decodedFrame, handleDeferredStage(decodedFrame, future))
                    .whenComplete(this::deferredResponseCompleted)
                    .thenApply(responseFilterCommand -> null);
        }
        else {
            return configureResponseFilterChain(decodedFrame, future)
                    .thenApply(responseFilterCommand -> null);
        }
    }

    private CompletableFuture<ResponseFilterCommand> dispatchDecodedResponseFrame(DecodedResponseFrame<?> decodedFrame, InternalFilterContext filterContext) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Dispatching upstream {} response to filter {}: {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }
        var stage = invoker.onResponse(decodedFrame.apiKey(), decodedFrame.apiVersion(),
                decodedFrame.header(), decodedFrame.body(), filterContext);
        if (stage == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{}: Filter{} for {} response unexpectedly returned null. This is a coding error in the filter. Closing connection.",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            closeConnection();
            return CompletableFuture.completedFuture(null);
        }
        return stage.toCompletableFuture();
    }

    private CompletableFuture<ResponseFilterCommand> configureResponseFilterChain(DecodedResponseFrame<?> decodedFrame, CompletableFuture<ResponseFilterCommand> future) {
        return future.thenApply(FilterHandler::validateFilterCommandNonNull)
                .thenApply(fr -> handleResponseFilterCommand(decodedFrame, fr))
                .exceptionally(t -> handleFilteringException(t, decodedFrame));
    }

    private CompletableFuture<Void> writeDecodedRequest(DecodedRequestFrame<?> decodedFrame, ChannelPromise promise) {
        var filterContext = new InternalFilterContext(decodedFrame, apiVersionService);
        final var future = dispatchDecodedRequest(decodedFrame, filterContext);
        boolean defer = !future.isDone();
        if (defer) {
            return configureRequestFilterChain(decodedFrame, promise, handleDeferredStage(decodedFrame, future))
                    .whenComplete(this::deferredRequestCompleted)
                    .thenApply(requestFilterCommand -> null);
        }
        else {
            return configureRequestFilterChain(decodedFrame, promise, future)
                    .thenApply(requestFilterCommand -> null);
        }
    }

    private CompletableFuture<RequestFilterCommand> dispatchDecodedRequest(DecodedRequestFrame<?> decodedFrame, InternalFilterContext filterContext) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Dispatching downstream {} request to filter{}: {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }

        var stage = invoker.onRequest(decodedFrame.apiKey(), decodedFrame.apiVersion(), decodedFrame.header(),
                decodedFrame.body(), filterContext);
        if (stage == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{}: Filter{} for {} request unexpectedly returned null. This is a coding error in the filter. Closing connection.",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            closeConnection();
            return CompletableFuture.completedFuture(null);
        }
        return stage.toCompletableFuture();
    }

    private CompletableFuture<RequestFilterCommand> configureRequestFilterChain(DecodedRequestFrame<?> decodedFrame, ChannelPromise promise,
                                                                                CompletableFuture<RequestFilterCommand> future) {
        return future.thenApply(FilterHandler::validateFilterCommandNonNull)
                .thenApply(fr -> handleRequestFilterCommand(decodedFrame, promise, fr))
                .exceptionally(t -> handleFilteringException(t, decodedFrame));
    }

    private ResponseFilterCommand handleResponseFilterCommand(DecodedResponseFrame<?> decodedFrame, ResponseFilterCommand responseFilterCommand) {
        if (responseFilterCommand.drop()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter{} drops {} response",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            return responseFilterCommand;
        }

        if (responseFilterCommand.message() != null) {
            ResponseHeaderData header = responseFilterCommand.header() == null ? decodedFrame.header() : (ResponseHeaderData) responseFilterCommand.header();
            forwardResponse(decodedFrame, header, responseFilterCommand.message());
        }

        if (responseFilterCommand.closeConnection()) {
            closeConnection();
        }
        return responseFilterCommand;
    }

    private RequestFilterCommand handleRequestFilterCommand(DecodedRequestFrame<?> decodedFrame, ChannelPromise promise, RequestFilterCommand requestFilterCommand) {
        if (requestFilterCommand.drop()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter{} drops {} request",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            return requestFilterCommand;
        }

        if (requestFilterCommand.message() != null) {
            if (requestFilterCommand.shortCircuitResponse()) {
                forwardShortCircuitResponse(decodedFrame, requestFilterCommand);
            }
            else {
                forwardRequest(decodedFrame, requestFilterCommand, promise);
            }
        }

        if (requestFilterCommand.closeConnection()) {
            if (requestFilterCommand.message() != null) {
                ctx.flush();
            }
            closeConnection();
        }
        return requestFilterCommand;
    }

    private <F extends FilterCommand> F handleFilteringException(Throwable t, DecodedFrame<?, ?> decodedFrame) {
        if (LOGGER.isWarnEnabled()) {
            var direction = decodedFrame.header() instanceof RequestHeaderData ? "request" : "response";
            LOGGER.warn("{}: Filter{} for {} {} ended exceptionally - closing connection",
                    channelDescriptor(), direction, filterDescriptor(), decodedFrame.apiKey(), t);
        }
        closeConnection();
        return null;
    }

    private <F extends FilterCommand> CompletableFuture<F> handleDeferredStage(DecodedFrame<?, ?> decodedFrame, CompletableFuture<F> future) {
        inboundChannel.config().setAutoRead(false);
        var timeoutFuture = ctx.executor().schedule(() -> {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{}: Filter {} was timed-out whilst processing {} {}", channelDescriptor(), filterDescriptor(),
                        decodedFrame instanceof DecodedRequestFrame ? "request" : "response", decodedFrame.apiKey());
            }
            future.completeExceptionally(new TimeoutException("Filter %s was timed-out.".formatted(filterDescriptor())));
        }, timeoutMs, TimeUnit.MILLISECONDS);
        return future.thenApply(FilterCommand -> {
            timeoutFuture.cancel(false);

            return FilterCommand;
        });
    }

    private void deferredResponseCompleted(ResponseFilterCommand ignored, Throwable throwable) {
        inboundChannel.config().setAutoRead(true);
        readFuture.whenComplete((u, t) -> inboundChannel.flush());
    }

    private void deferredRequestCompleted(RequestFilterCommand ignored, Throwable throwable) {
        inboundChannel.config().setAutoRead(true);
        // flush so that writes from this completion can be driven towards the broker
        ctx.flush();
        // chain a flush to force any pending writes towards the broker
        writeFuture.whenComplete((u, t) -> ctx.flush());
        // flush inbound in case of short-circuit
        inboundChannel.flush();
    }

    private void forwardRequest(DecodedRequestFrame<?> decodedFrame, RequestFilterCommand requestFilterCommand, ChannelPromise promise) {
        var header = requestFilterCommand.header() == null ? decodedFrame.header() : requestFilterCommand.header();
        ApiMessage message = requestFilterCommand.message();
        if (decodedFrame.body() != message) {
            throw new IllegalStateException();
        }
        if (decodedFrame.header() != header) {
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
        ctx.write(decodedFrame, promise);
    }

    private void forwardResponse(DecodedFrame<?, ?> decodedFrame, ResponseHeaderData header, ApiMessage message) {
        // check it's a response
        String name = message.getClass().getName();
        if (!name.endsWith("ResponseData")) {
            throw new AssertionError("Attempt to use forwardResponse with a non-response: " + name);
        }
        if (decodedFrame instanceof RequestFrame) {
            if (message.apiKey() != decodedFrame.apiKey().id) {
                throw new AssertionError(
                        "Attempt to respond with ApiMessage of type " + ApiKeys.forId(message.apiKey()) + " but request is of type " + decodedFrame.apiKey());
            }
            DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(decodedFrame.apiVersion(), decodedFrame.correlationId(),
                    header, message);
            decodedFrame.transferBuffersTo(responseFrame);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Forwarding response: {}", channelDescriptor(), decodedFrame);
            }
            ctx.fireChannelRead(responseFrame);
            // required to flush the message back to the client
            ctx.fireChannelReadComplete();
        }
        else {
            if (decodedFrame.body() != message) {
                throw new AssertionError();
            }
            if (decodedFrame.header() != header) {
                throw new AssertionError();
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Forwarding response: {}", channelDescriptor(), decodedFrame);
            }
            ctx.fireChannelRead(decodedFrame);
        }
    }

    private void forwardShortCircuitResponse(DecodedRequestFrame<?> decodedFrame, RequestFilterCommand requestFilterCommand) {
        if (decodedFrame.hasResponse()) {
            var header = requestFilterCommand.header() == null ? new ResponseHeaderData() : ((ResponseHeaderData) requestFilterCommand.header());
            header.setCorrelationId(decodedFrame.correlationId());
            forwardResponse(decodedFrame, header, requestFilterCommand.message());
        }
        else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter {} attempted to short-circuit respond to a message with apiKey {}" +
                        " that has no response in the Kafka Protocol, dropping response",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
        }
    }

    private void closeConnection() {
        ctx.close().addListener(future -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Channel closed", channelDescriptor());
            }
        });
    }

    private String channelDescriptor() {
        return ctx.channel().toString();
    }

    private void completeInternalResponse(InternalResponseFrame<?> decodedFrame) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Completing {} response for request sent by this filter{}: {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }
        CompletableFuture<ApiMessage> p = decodedFrame.promise();
        p.complete(decodedFrame.body());
    }

    private static <F extends FilterCommand> F validateFilterCommandNonNull(F f) {
        return Objects.requireNonNullElseGet(f, () -> {
            throw new IllegalStateException("filter completion must not yield a null result");
        });
    }

    private class InternalFilterContext implements FilterContext, ApiVersionsService {

        private final DecodedFrame<?, ?> decodedFrame;
        private final ApiVersionsServiceImpl apiVersionService;

        InternalFilterContext(DecodedFrame<?, ?> decodedFrame, ApiVersionsServiceImpl apiVersionService) {
            this.decodedFrame = decodedFrame;
            this.apiVersionService = apiVersionService;
        }

        @Override
        public String channelDescriptor() {
            return FilterHandler.this.channelDescriptor();
        }

        @Override
        public ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity) {
            final ByteBuf buffer = ctx.alloc().ioBuffer(initialCapacity);
            decodedFrame.add(buffer);
            return new ByteBufOutputStream(buffer);
        }

        @Nullable
        @Override
        public String sniHostname() {
            return sniHostname;
        }

        public String getVirtualClusterName() {
            return virtualCluster.getClusterName();
        }

        @Override
        public ApiVersionsService getApiVersionsService() {
            return this;
        }

        @Override
        public RequestFilterCommandBuilder requestFilterCommandBuilder() {
            return new RequestFilterCommandBuilderImpl();
        }

        @Override
        public ResponseFilterCommandBuilder responseFilterCommandBuilder() {
            return new ResponseFilterCommandBuilderImpl();
        }

        @Override
        public CompletionStage<RequestFilterCommand> forwardRequest(RequestHeaderData header, ApiMessage request) {
            return requestFilterCommandBuilder().forward(header, request).completed();
        }

        @Override
        public CompletionStage<ResponseFilterCommand> forwardResponse(ResponseHeaderData header, ApiMessage response) {
            return responseFilterCommandBuilder().forward(header, response).completed();
        }

        @Override
        public <T extends ApiMessage> CompletionStage<T> sendRequest(short apiVersion, ApiMessage request) {
            short key = request.apiKey();
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
                    || ((ProduceRequestData) request).acks() != 0;
            var filterPromise = new CompletableFuture<T>();
            var filterStage = new InternalCompletionStage<>(filterPromise);
            var frame = new InternalRequestFrame<>(
                    apiVersion, -1, hasResponse,
                    filter, filterPromise, header, request);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Sending request: {}", FilterHandler.this.channelDescriptor(), frame);
            }
            ChannelPromise writePromise = ctx.channel().newPromise();
            ctx.writeAndFlush(frame, writePromise);

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

            ctx.executor().schedule(() -> {
                LOGGER.debug("{}: Timing out {} request after {}ms", ctx, apiKey, timeoutMs);
                filterPromise
                        .completeExceptionally(new TimeoutException("Asynchronous %s request made by filter %s was timed-out.".formatted(apiKey, filterDescriptor())));
            }, timeoutMs, TimeUnit.MILLISECONDS);
            return filterStage;
        }

        @Override
        public CompletionStage<Optional<ApiVersionRanges>> getApiVersionRanges(ApiKeys keys) {
            return apiVersionService.getApiVersionRanges(keys, this);
        }

    }
}
