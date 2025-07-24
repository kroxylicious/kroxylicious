/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

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
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.filter.RequestFilterResultBuilderImpl;
import io.kroxylicious.proxy.internal.filter.ResponseFilterResultBuilderImpl;
import io.kroxylicious.proxy.internal.util.Assertions;
import io.kroxylicious.proxy.internal.util.ByteBufOutputStream;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@code ChannelInboundHandler} (for handling requests from downstream)
 * that applies a single {@link Filter}.
 */
public class FilterHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);
    private final long timeoutMs;
    private final String sniHostname;
    private final VirtualClusterModel virtualClusterModel;
    private final Channel inboundChannel;
    private final FilterAndInvoker filterAndInvoker;
    private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> readFuture = CompletableFuture.completedFuture(null);
    private ChannelHandlerContext ctx;
    private PromiseFactory promiseFactory;

    public FilterHandler(FilterAndInvoker filterAndInvoker, long timeoutMs, String sniHostname, VirtualClusterModel virtualClusterModel, Channel inboundChannel) {
        this.filterAndInvoker = Objects.requireNonNull(filterAndInvoker);
        this.timeoutMs = Assertions.requireStrictlyPositive(timeoutMs, "timeout");
        this.sniHostname = sniHostname;
        this.virtualClusterModel = virtualClusterModel;
        this.inboundChannel = inboundChannel;
    }

    @Override
    public String toString() {
        return "FilterHandler{" +
                filterDescriptor() +
                '}';
    }

    String filterDescriptor() {
        return filterAndInvoker.filterName();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        this.promiseFactory = new PromiseFactory(ctx.executor(), timeoutMs, TimeUnit.MILLISECONDS, LOGGER.getName());
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof InternalResponseFrame<?> decodedFrame) {
            // jump the queue, let responses to asynchronous requests flow back to their sender
            if (decodedFrame.isRecipient(filterAndInvoker.filter())) {
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
            if (msg instanceof OpaqueResponseFrame orf) {
                readFuture = readFuture.whenComplete((a, b) -> {
                    if (ctx.channel().isOpen()) {
                        ctx.fireChannelRead(msg);
                    }
                    else {
                        orf.releaseBuffer();
                    }
                });
            }
            else {
                throw new IllegalStateException("Filter '" + filterAndInvoker.filterName() + "': Unexpected message reading from upstream: " + msgDescriptor(msg));
            }
        }
    }

    /**
     * @param obj A message
     * @return A descriptor for the message (for logging purposes). Does not include the message contents.
     */
    static String msgDescriptor(@Nullable Object obj) {
        if (obj == null) {
            return "«null»";
        }
        else if (obj instanceof DecodedFrame<?, ?> df) {
            return df.getClass().getSimpleName() + "(" + df.apiKey() + "@" + df.apiVersion() + " corrId=" + df.correlationId() + ")";
        }
        else if (obj instanceof OpaqueFrame of) {
            return of.toString();
        }
        else {
            return obj.getClass().getName();
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
            if (msg instanceof OpaqueRequestFrame || msg == Unpooled.EMPTY_BUFFER) {
                writeFuture = writeFuture.whenComplete((unused, throwable) -> {
                    if (ctx.channel().isOpen()) {
                        ctx.write(msg, promise);
                    }
                    else if (msg instanceof OpaqueRequestFrame orf) {
                        orf.releaseBuffer();
                    }
                });
            }
            else {
                // Unpooled.EMPTY_BUFFER is used by KafkaProxyFrontendHandler#closeOnFlush
                // but, otherwise we don't expect any other kind of message
                throw new IllegalStateException("Filter '" + filterAndInvoker.filterName() + "': Unexpected message writing to upstream: " + msgDescriptor(msg));
            }
        }
    }

    private CompletableFuture<Void> readDecodedResponse(DecodedResponseFrame<?> decodedFrame) {
        var filterContext = new InternalFilterContext(decodedFrame);

        final var future = dispatchDecodedResponseFrame(decodedFrame, filterContext);
        boolean defer = !future.isDone();
        if (defer) {
            return configureResponseFilterChain(decodedFrame, handleDeferredStage(decodedFrame, future))
                    .whenComplete(this::deferredResponseCompleted)
                    .thenApply(responseFilterResult -> null);
        }
        else {
            return configureResponseFilterChain(decodedFrame, future)
                    .thenApply(responseFilterResult -> null);
        }
    }

    private CompletableFuture<ResponseFilterResult> dispatchDecodedResponseFrame(DecodedResponseFrame<?> decodedFrame, InternalFilterContext filterContext) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Dispatching upstream {} response to filter '{}': {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }
        var stage = filterAndInvoker.invoker().onResponse(decodedFrame.apiKey(), decodedFrame.apiVersion(),
                decodedFrame.header(), decodedFrame.body(), filterContext);
        return stage instanceof InternalCompletionStage ? ((InternalCompletionStage<ResponseFilterResult>) stage).getUnderlyingCompletableFuture()
                : stage.toCompletableFuture();
    }

    private CompletableFuture<ResponseFilterResult> configureResponseFilterChain(DecodedResponseFrame<?> decodedFrame, CompletableFuture<ResponseFilterResult> future) {
        return future.thenApply(FilterHandler::validateFilterResultNonNull)
                .thenApply(fr -> handleResponseFilterResult(decodedFrame, fr))
                .exceptionally(t -> handleFilteringException(t, decodedFrame));
    }

    private CompletableFuture<Void> writeDecodedRequest(DecodedRequestFrame<?> decodedFrame, ChannelPromise promise) {
        var filterContext = new InternalFilterContext(decodedFrame);
        final var future = dispatchDecodedRequest(decodedFrame, filterContext);
        boolean defer = !future.isDone();
        if (defer) {
            return configureRequestFilterChain(decodedFrame, promise, handleDeferredStage(decodedFrame, future))
                    .whenComplete(this::deferredRequestCompleted)
                    .thenApply(requestFilterResult -> null);
        }
        else {
            return configureRequestFilterChain(decodedFrame, promise, future)
                    .thenApply(requestFilterResult -> null);
        }
    }

    private CompletableFuture<RequestFilterResult> dispatchDecodedRequest(DecodedRequestFrame<?> decodedFrame, InternalFilterContext filterContext) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Dispatching downstream {} request to filter '{}': {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }

        var stage = filterAndInvoker.invoker().onRequest(decodedFrame.apiKey(), decodedFrame.apiVersion(), decodedFrame.header(),
                decodedFrame.body(), filterContext);
        return stage instanceof InternalCompletionStage ? ((InternalCompletionStage<RequestFilterResult>) stage).getUnderlyingCompletableFuture()
                : stage.toCompletableFuture();
    }

    private CompletableFuture<RequestFilterResult> configureRequestFilterChain(DecodedRequestFrame<?> decodedFrame, ChannelPromise promise,
                                                                               CompletableFuture<RequestFilterResult> future) {
        return future.thenApply(FilterHandler::validateFilterResultNonNull)
                .thenApply(fr -> handleRequestFilterResult(decodedFrame, promise, fr))
                .exceptionally(t -> handleFilteringException(t, decodedFrame));
    }

    private ResponseFilterResult handleResponseFilterResult(DecodedResponseFrame<?> decodedFrame, ResponseFilterResult responseFilterResult) {
        if (responseFilterResult.drop()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' drops {} response",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            return responseFilterResult;
        }

        if (responseFilterResult.message() != null) {
            ResponseHeaderData header = responseFilterResult.header() == null ? decodedFrame.header() : (ResponseHeaderData) responseFilterResult.header();
            forwardResponse(decodedFrame, header, responseFilterResult.message());
        }

        if (responseFilterResult.closeConnection()) {
            closeConnection();
        }
        return responseFilterResult;
    }

    private RequestFilterResult handleRequestFilterResult(DecodedRequestFrame<?> decodedFrame, ChannelPromise promise, RequestFilterResult requestFilterResult) {
        if (requestFilterResult.drop()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' drops {} request",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            return requestFilterResult;
        }

        if (requestFilterResult.message() != null) {
            if (requestFilterResult.shortCircuitResponse()) {
                forwardShortCircuitResponse(decodedFrame, requestFilterResult);
            }
            else {
                forwardRequest(decodedFrame, requestFilterResult, promise);
            }
        }

        if (requestFilterResult.closeConnection()) {
            if (requestFilterResult.message() != null) {
                ctx.flush();
            }
            closeConnection();
        }
        return requestFilterResult;
    }

    private <F extends FilterResult> F handleFilteringException(Throwable t, DecodedFrame<?, ?> decodedFrame) {
        if (LOGGER.isWarnEnabled()) {
            var direction = decodedFrame.header() instanceof RequestHeaderData ? "request" : "response";
            LOGGER.atWarn().setMessage("{}: Filter '{}' for {} {} ended exceptionally - closing connection. Cause message {}")
                    .addArgument(channelDescriptor())
                    .addArgument(direction)
                    .addArgument(filterDescriptor())
                    .addArgument(decodedFrame.apiKey())
                    .addArgument(t.getMessage())
                    .setCause(LOGGER.isDebugEnabled() ? t : null)
                    .log();
        }
        closeConnection();
        return null;
    }

    private <F extends FilterResult> CompletableFuture<F> handleDeferredStage(DecodedFrame<?, ?> decodedFrame, CompletableFuture<F> future) {
        inboundChannel.config().setAutoRead(false);
        promiseFactory.wrapWithTimeLimit(future,
                () -> "Deferred work for filter '%s' did not complete processing within %s ms %s %s".formatted(filterDescriptor(), timeoutMs,
                        decodedFrame instanceof DecodedRequestFrame ? "request" : "response", decodedFrame.apiKey()));
        return future.thenApplyAsync(filterResult -> filterResult, ctx.executor());
    }

    private void deferredResponseCompleted(ResponseFilterResult ignored, Throwable throwable) {
        inboundChannel.config().setAutoRead(true);
        readFuture.whenComplete((u, t) -> inboundChannel.flush());
    }

    private void deferredRequestCompleted(RequestFilterResult ignored, Throwable throwable) {
        inboundChannel.config().setAutoRead(true);
        // flush so that writes from this completion can be driven towards the broker
        ctx.flush();
        // chain a flush to force any pending writes towards the broker
        writeFuture.whenComplete((u, t) -> ctx.flush());
        // flush inbound in case of short-circuit
        inboundChannel.flush();
    }

    private void forwardRequest(DecodedRequestFrame<?> decodedFrame, RequestFilterResult requestFilterResult, ChannelPromise promise) {
        var header = requestFilterResult.header() == null ? decodedFrame.header() : requestFilterResult.header();
        ApiMessage message = requestFilterResult.message();
        if (decodedFrame.body() != message) {
            throw new IllegalStateException();
        }
        if (decodedFrame.header() != header) {
            throw new IllegalStateException();
        }
        // check it's a request
        String name = message.getClass().getName();
        if (!name.endsWith("RequestData")) {
            throw new AssertionError("Filter '" + filterDescriptor() + "': Attempt to use forwardRequest with a non-request: " + name);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Filter '{}' forwarding request: {}", channelDescriptor(), filterDescriptor(), decodedFrame);
        }
        ctx.write(decodedFrame, promise);
    }

    private void forwardResponse(DecodedFrame<?, ?> decodedFrame, ResponseHeaderData header, ApiMessage message) {
        // check it's a response
        String name = message.getClass().getName();
        if (!name.endsWith("ResponseData")) {
            throw new AssertionError("Filter '" + filterDescriptor() + "': Attempt to use forwardResponse with a non-response: " + name);
        }
        if (decodedFrame instanceof RequestFrame) {
            if (message.apiKey() != decodedFrame.apiKeyId()) {
                throw new AssertionError(
                        "Filter '" + filterDescriptor() + "': Attempt to respond with ApiMessage of type " + ApiKeys.forId(message.apiKey()) + " but request is of type "
                                + decodedFrame.apiKey());
            }
            DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(decodedFrame.apiVersion(), decodedFrame.correlationId(),
                    header, message);
            decodedFrame.transferBuffersTo(responseFrame);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' forwarding response: {}", channelDescriptor(), filterDescriptor(), msgDescriptor(decodedFrame));
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
                LOGGER.debug("{}: Filter '{}' forwarding response: {}", channelDescriptor(), filterDescriptor(), msgDescriptor(decodedFrame));
            }
            ctx.fireChannelRead(decodedFrame);
        }
    }

    private void forwardShortCircuitResponse(DecodedRequestFrame<?> decodedFrame, RequestFilterResult requestFilterResult) {
        if (decodedFrame.hasResponse()) {
            var header = requestFilterResult.header() == null ? new ResponseHeaderData() : ((ResponseHeaderData) requestFilterResult.header());
            header.setCorrelationId(decodedFrame.correlationId());
            forwardResponse(decodedFrame, header, requestFilterResult.message());
        }
        else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' attempted to short-circuit respond to a message with apiKey {}" +
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

    @SuppressWarnings("unchecked")
    private void completeInternalResponse(InternalResponseFrame<?> decodedFrame) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Completing {} response for request to filter '{}': {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }
        CompletableFuture<ApiMessage> p = (CompletableFuture<ApiMessage>) decodedFrame
                .promise();
        p.complete(decodedFrame.body());
    }

    private static <F extends FilterResult> F validateFilterResultNonNull(F f) {
        return Objects.requireNonNullElseGet(f, () -> {
            throw new IllegalStateException("Filter completion must not yield a null result");
        });
    }

    private class InternalFilterContext implements FilterContext {

        private final DecodedFrame<?, ?> decodedFrame;

        InternalFilterContext(DecodedFrame<?, ?> decodedFrame) {
            this.decodedFrame = decodedFrame;
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
            return virtualClusterModel.getClusterName();
        }

        @Override
        public @NonNull Optional<ClientTlsContext> clientTlsContext() {
            return Optional.ofNullable(inboundChannel.pipeline().get(SslHandler.class))
                    .map(clientFacingSslHandler -> new ClientTlsContextImpl(
                            Objects.requireNonNull(localTlsCertificate(clientFacingSslHandler)), getPeerTlsCertificate(clientFacingSslHandler)));
        }

        @Override
        public RequestFilterResultBuilder requestFilterResultBuilder() {
            return new RequestFilterResultBuilderImpl();
        }

        @Override
        public ResponseFilterResultBuilder responseFilterResultBuilder() {
            return new ResponseFilterResultBuilderImpl();
        }

        @Override
        public CompletionStage<RequestFilterResult> forwardRequest(RequestHeaderData header, ApiMessage request) {
            return requestFilterResultBuilder().forward(header, request).completed();
        }

        @Override
        public CompletionStage<ResponseFilterResult> forwardResponse(ResponseHeaderData header, ApiMessage response) {
            return responseFilterResultBuilder().forward(header, response).completed();
        }

        @NonNull
        @Override
        public <M extends ApiMessage> CompletionStage<M> sendRequest(@NonNull RequestHeaderData header,
                                                                     @NonNull ApiMessage request) {
            Objects.requireNonNull(header);
            Objects.requireNonNull(request);

            var apiKey = ApiKeys.forId(request.apiKey());
            header.setRequestApiKey(apiKey.id);
            header.setCorrelationId(-1);

            if (!apiKey.isVersionSupported(header.requestApiVersion())) {
                throw new IllegalArgumentException(
                        "Filter '%s': apiKey %s does not support version %d. the supported version range for this api key is %d...%d (inclusive)."
                                .formatted(filterDescriptor(), apiKey, header.requestApiVersion(), apiKey.oldestVersion(), apiKey.latestVersion()));
            }

            var hasResponse = apiKey != ApiKeys.PRODUCE || ((ProduceRequestData) request).acks() != 0;
            CompletableFuture<M> filterPromise = promiseFactory.newTimeLimitedPromise(
                    () -> "Asynchronous %s request made by filter '%s' failed to complete within %s ms.".formatted(apiKey, filterDescriptor(), timeoutMs));
            var frame = new InternalRequestFrame<>(
                    header.requestApiVersion(), header.correlationId(), hasResponse,
                    filterAndInvoker.filter(), filterPromise, header, request);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' sending request: {}", FilterHandler.this.channelDescriptor(), filterDescriptor(), msgDescriptor(frame));
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

            return filterPromise.minimalCompletionStage();
        }

    }

    @VisibleForTesting
    static @Nullable X509Certificate getPeerTlsCertificate(@Nullable SslHandler sslHandler) {
        if (sslHandler != null) {
            SSLSession session = sslHandler.engine().getSession();

            Certificate[] peerCertificates;
            try {
                peerCertificates = session.getPeerCertificates();
            }
            catch (SSLPeerUnverifiedException e) {
                peerCertificates = null;
            }
            if (peerCertificates != null && peerCertificates.length > 0) {
                return Objects.requireNonNull((X509Certificate) peerCertificates[0]);
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

    @VisibleForTesting
    static @Nullable X509Certificate localTlsCertificate(@Nullable SslHandler sslHandler) {
        if (sslHandler != null) {
            SSLSession session = sslHandler.engine().getSession();
            Certificate[] localCertificates = session.getLocalCertificates();
            if (localCertificates != null && localCertificates.length > 0) {
                return Objects.requireNonNull((X509Certificate) localCertificates[0]);
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

}
