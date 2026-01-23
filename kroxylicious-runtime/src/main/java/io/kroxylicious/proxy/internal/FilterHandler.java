/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.Uuid;
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

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.filter.RequestFilterResultBuilderImpl;
import io.kroxylicious.proxy.internal.filter.ResponseFilterResultBuilderImpl;
import io.kroxylicious.proxy.internal.util.Assertions;
import io.kroxylicious.proxy.internal.util.ByteBufOutputStream;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@code ChannelInboundHandler} (for handling requests from downstream)
 * that applies a single {@link Filter}.
 */
public class FilterHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);
    private final long timeoutMs;
    private final @Nullable String sniHostname;
    private final VirtualClusterModel virtualClusterModel;
    private final Channel inboundChannel;
    private final FilterAndInvoker filterAndInvoker;
    private final ProxyChannelStateMachine proxyChannelStateMachine;
    private final ClientSubjectManager clientSubjectManager;

    /** Chains response processing to preserve ordering when filters defer work asynchronously. */
    private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);

    /** Chains request processing to preserve ordering when filters defer work asynchronously. */
    private CompletableFuture<Void> readFuture = CompletableFuture.completedFuture(null);

    /**
     * Set in {@link #handlerAdded}. Guaranteed non-null when handler methods execute
     * per Netty's lifecycle contract.
     * Applies to {@link #promiseFactory} as well.
     */
    private @Nullable ChannelHandlerContext ctx;
    private @Nullable PromiseFactory promiseFactory;

    private static final AtomicBoolean deprecationWarningEmitted = new AtomicBoolean(false);

    public FilterHandler(FilterAndInvoker filterAndInvoker,
                         long timeoutMs,
                         @Nullable String sniHostname,
                         VirtualClusterModel virtualClusterModel,
                         Channel inboundChannel,
                         ProxyChannelStateMachine proxyChannelStateMachine,
                         ClientSubjectManager clientSubjectManager) {
        this.filterAndInvoker = Objects.requireNonNull(filterAndInvoker);
        this.timeoutMs = Assertions.requireStrictlyPositive(timeoutMs, "timeout");
        this.sniHostname = sniHostname;
        this.virtualClusterModel = virtualClusterModel;
        this.inboundChannel = inboundChannel;
        this.proxyChannelStateMachine = proxyChannelStateMachine;
        this.clientSubjectManager = clientSubjectManager;
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
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        this.promiseFactory = new PromiseFactory(ctx.channel().eventLoop(), timeoutMs, TimeUnit.MILLISECONDS, LOGGER.getName());
        super.handlerAdded(ctx);
    }

    /**
     * Handles outbound responses flowing toward the client.
     * outbound writes are requests that can succeed or fail. The promise allows the
     * original writer to be notified when data reaches the socket (or if an error occurs).
     *
     * @param ctx channel handler context for each filter handler
     * @param msg the message being written
     * @param promise the channel promise
     * @throws Exception if an error occurs
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof InternalResponseFrame<?> decodedFrame) {
            // jump the queue, let responses to asynchronous requests flow back to their sender
            if (decodedFrame.isRecipient(filterAndInvoker.filter())) {
                completeInternalResponse(decodedFrame);
            }
            else {
                handleDecodedResponse(decodedFrame, promise);
            }
        }
        else if (msg instanceof DecodedResponseFrame<?> decodedFrame) {
            if (writeFuture.isDone()) {
                writeFuture = handleDecodedResponse(decodedFrame, promise);
            }
            else {
                writeFuture = writeFuture.thenCompose(ignored -> {
                    if (ctx.channel().isOpen()) {
                        return handleDecodedResponse(decodedFrame, promise);
                    }
                    else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).exceptionally(throwable -> null);
            }
        }
        else {
            if (msg instanceof OpaqueResponseFrame orf) {
                writeFuture = writeFuture.whenComplete((a, b) -> {
                    if (ctx.channel().isOpen()) {
                        ctx.write(msg, promise);
                    }
                    else {
                        orf.releaseBuffer();
                    }
                });
            }
            else {
                throw new IllegalStateException("Filter '" + filterAndInvoker.filterName() + "': Unexpected message writing to downstream: " + msgDescriptor(msg));
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

    /**
     * Handles inbound requests flowing toward the upstream broker.
     * @param ctx channel handler context for each filter handler
     * @param msg the message being read
     * @throws Exception if an error occurs
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof InternalRequestFrame<?> decodedFrame) {
            // jump the queue, internal request must flow!
            handleDecodedRequest(decodedFrame);
        }
        else if (msg instanceof DecodedRequestFrame<?> decodedFrame) {
            if (readFuture.isDone()) {
                readFuture = handleDecodedRequest(decodedFrame);
            }
            else {
                readFuture = readFuture.thenCompose(ignored -> {
                    if (ctx.channel().isOpen()) {
                        return handleDecodedRequest(decodedFrame);
                    }
                    else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).exceptionally(throwable -> null);
            }
        }
        else {
            if (msg instanceof OpaqueRequestFrame || msg == Unpooled.EMPTY_BUFFER) {
                readFuture = readFuture.whenComplete((unused, throwable) -> {
                    if (ctx.channel().isOpen()) {
                        ctx.fireChannelRead(msg);
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

    private CompletableFuture<Void> handleDecodedResponse(DecodedResponseFrame<?> decodedFrame, ChannelPromise promise) {
        var filterContext = new InternalFilterContext(decodedFrame);

        final var future = dispatchDecodedResponseFrame(decodedFrame, filterContext);
        boolean defer = !future.isDone();
        if (defer) {
            return configureResponseFilterChain(decodedFrame, promise, handleDeferredStage(decodedFrame, future))
                    .whenComplete(this::deferredResponseCompleted)
                    .thenApply(responseFilterResult -> null);
        }
        else {
            return configureResponseFilterChain(decodedFrame, promise, future)
                    .thenApply(responseFilterResult -> null);
        }
    }

    private CompletableFuture<ResponseFilterResult> dispatchDecodedResponseFrame(DecodedResponseFrame<?> decodedFrame,
                                                                                 InternalFilterContext filterContext) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Dispatching upstream {} response to filter '{}': {}",
                    channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
        }
        var stage = filterAndInvoker.invoker().onResponse(decodedFrame.apiKey(), decodedFrame.apiVersion(),
                decodedFrame.header(), decodedFrame.body(), filterContext);
        return stage.toCompletableFuture();
    }

    private CompletableFuture<ResponseFilterResult> configureResponseFilterChain(DecodedResponseFrame<?> decodedFrame,
                                                                                 ChannelPromise promise,
                                                                                 CompletableFuture<ResponseFilterResult> future) {
        return future.thenApply(FilterHandler::validateFilterResultNonNull)
                .thenApply(fr -> handleResponseFilterResult(decodedFrame, fr, promise))
                .exceptionally(t -> handleFilteringException(t, decodedFrame));
    }

    /**
     * Handle a decoded request frame through the filter chain.
     * If the promise is null, propagate inbound (fireChannelRead), else write upstream.
     * @param decodedFrame the decoded frame
     * @return a future that completes when processing is complete
     */
    private CompletableFuture<Void> handleDecodedRequest(DecodedRequestFrame<?> decodedFrame) {
        var filterContext = new InternalFilterContext(decodedFrame);
        final var future = dispatchDecodedRequest(decodedFrame, filterContext);
        boolean defer = !future.isDone();
        if (defer) {
            return configureRequestFilterChain(decodedFrame, handleDeferredStage(decodedFrame, future))
                    .whenComplete(this::deferredRequestCompleted)
                    .thenApply(requestFilterResult -> null);
        }
        else {
            return configureRequestFilterChain(decodedFrame, future)
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
        return stage.toCompletableFuture();
    }

    private CompletableFuture<RequestFilterResult> configureRequestFilterChain(DecodedRequestFrame<?> decodedFrame,
                                                                               CompletableFuture<RequestFilterResult> future) {
        return future.thenApply(FilterHandler::validateFilterResultNonNull)
                .thenApply(fr -> handleRequestFilterResult(decodedFrame, fr))
                .exceptionally(t -> handleFilteringException(t, decodedFrame));
    }

    private ResponseFilterResult handleResponseFilterResult(DecodedResponseFrame<?> decodedFrame, ResponseFilterResult responseFilterResult,
                                                            @Nullable ChannelPromise promise) {
        if (responseFilterResult.drop()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' drops {} response",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            return responseFilterResult;
        }

        ApiMessage message = responseFilterResult.message();
        if (message != null) {
            ResponseHeaderData header = responseFilterResult.header() == null ? decodedFrame.header()
                    : (ResponseHeaderData) Objects.requireNonNull(responseFilterResult.header());
            forwardResponse(decodedFrame, header, message, promise);
        }

        if (responseFilterResult.closeConnection()) {
            if (responseFilterResult.message() != null) {
                ctx.flush(); // ensure writes are flushed before closing
            }
            closeConnection();
        }
        return responseFilterResult;
    }

    private RequestFilterResult handleRequestFilterResult(DecodedRequestFrame<?> decodedFrame,
                                                          RequestFilterResult requestFilterResult) {
        if (requestFilterResult.drop()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' drops {} request",
                        channelDescriptor(), filterDescriptor(), decodedFrame.apiKey());
            }
            // When a request is dropped, trigger reading the next request to keep the channel active
            inboundChannel.read();
            return requestFilterResult;
        }

        if (requestFilterResult.message() != null) {
            if (requestFilterResult.shortCircuitResponse()) {
                forwardShortCircuitResponse(decodedFrame, requestFilterResult);
                inboundChannel.read();
            }
            else {
                forwardRequest(decodedFrame, requestFilterResult);
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

    private <F extends FilterResult> @Nullable F handleFilteringException(Throwable t, DecodedFrame<?, ?> decodedFrame) {
        if (LOGGER.isWarnEnabled()) {
            var direction = decodedFrame.header() instanceof RequestHeaderData ? "request" : "response";
            LOGGER.atWarn().setMessage("{}: Filter '{}' for {} {} ended exceptionally - closing connection. Cause message {}")
                    .addArgument(proxyChannelStateMachine.sessionId())
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

    /**
     * Called when a deferred response filter operation completes.
     * Unlike {@link #deferredRequestCompleted}, no immediate flush is needed here
     * because responses always flow through the normal write path with its own flush handling.
     */
    private void deferredResponseCompleted(ResponseFilterResult ignored, Throwable throwable) {
        inboundChannel.config().setAutoRead(true);
        // Ensure proper ordering of flushes to prevent race conditions
        writeFuture.whenComplete((u, t) -> {
            ctx.flush();
            readFuture.whenComplete((u2, t2) -> inboundChannel.flush());
        });
    }

    /**
     * Called when a deferred (async) request filter operation completes.
     * <p>
     * Re-enables auto-read and ensures all pending writes are flushed.
     * <p>
     * <p><b>Why two flushes?</b>
     * <pre>
     * ctx.flush();                          // FLUSH #1: Immediate
     * writeFuture.whenComplete((u, t) -> {
     *     ctx.flush();                      // FLUSH #2: After pending writes complete
     *     inboundChannel.flush();
     * });
     * </pre>
     * <ul>
     *   <li><b>FLUSH #1:</b> Handles short-circuit responses where {@code ctx.write()} already
     *       happened synchronously. Ensures response is sent to client immediately.</li>
     *   <li><b>FLUSH #2:</b> Handles async response writes that may complete after this method
     *       returns. Waits for {@code writeFuture} to ensure all chained writes are flushed.</li>
     * </ul>
     * If no writes occurred, flush is a no-op (harmless). This belt-and-suspenders approach
     * prevents race conditions between async writes and flush timing.
     */
    private void deferredRequestCompleted(RequestFilterResult ignored, Throwable throwable) {
        inboundChannel.config().setAutoRead(true);
        // Ensure proper ordering of flushes to prevent race conditions
        // First flush any immediate writes, then chain additional flushes
        ctx.flush();
        writeFuture.whenComplete((u, t) -> {
            ctx.flush();
            // flush inbound in case of short-circuit, but only after context flush is done
            inboundChannel.flush();
        });
    }

    private void forwardRequest(DecodedRequestFrame<?> decodedFrame,
                                RequestFilterResult requestFilterResult) {
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

        ctx.fireChannelRead(decodedFrame);
        ctx.fireChannelReadComplete();
    }

    /**
     * Forwards a response toward the client.
     *
     * @param decodedFrame The decoded frame to respond to.
     * @param header       The response header.
     * @param message      The response message.
     * @param promise The write promise from upstream, or {@code null} for short-circuit responses.
     * <p>
     * <b>Why nullable?</b>
     * <ul>
     *     <li><b>Non-null:</b> Normal response path — promise originated from an upstream
     *         write() call and must be passed through so the original writer gets notified.</li>
     *     <li><b>Null:</b> Short-circuit path — filter generated this response locally
     *         (no broker round-trip), so no upstream writer is waiting for completion.</li>
     * </ul>
     * <p>
     * When null, we use {@code ctx.voidPromise()} (avoids allocation) and flush
     * immediately since no one else will trigger the flush.
     */
    private void forwardResponse(DecodedFrame<?, ?> decodedFrame, ResponseHeaderData header, ApiMessage message, @Nullable ChannelPromise promise) {
        // check it's a response
        String name = message.getClass().getName();
        if (!name.endsWith("ResponseData")) {
            throw new AssertionError("Filter '" + filterDescriptor() + "': Attempt to use forwardResponse with a non-response: " + name);
        }
        if (decodedFrame instanceof DecodedRequestFrame<?> decodedRequestFrame) {
            if (message.apiKey() != decodedFrame.apiKeyId()) {
                throw new AssertionError(
                        "Filter '" + filterDescriptor() + "': Attempt to respond with ApiMessage of type " + ApiKeys.forId(message.apiKey()) + " but request is of type "
                                + decodedFrame.apiKey());
            }
            DecodedResponseFrame<?> responseFrame = decodedRequestFrame.responseFrame(header, message);
            decodedFrame.transferBuffersTo(responseFrame);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: Filter '{}' forwarding response: {}", channelDescriptor(), filterDescriptor(), msgDescriptor(decodedFrame));
            }
            ctx.write(responseFrame, promise != null ? promise : ctx.voidPromise());
            if (promise == null) {
                ctx.flush();
            }
        }
        else {
            if (decodedFrame.body() != message) {
                throw new AssertionError();
            }
            if (decodedFrame.header() != header) {
                throw new AssertionError();
            }
            if (LOGGER.isDebugEnabled()) {
                // noinspection LoggingSimilarMessage
                LOGGER.debug("{}: Filter '{}' forwarding response: {}", channelDescriptor(), filterDescriptor(), msgDescriptor(decodedFrame));
            }
            ctx.write(decodedFrame, promise != null ? promise : ctx.voidPromise());
        }
    }

    private void forwardShortCircuitResponse(DecodedRequestFrame<?> decodedFrame, RequestFilterResult requestFilterResult) {
        if (decodedFrame.hasResponse()) {
            var header = requestFilterResult.header() == null ? new ResponseHeaderData() : Objects.requireNonNull((ResponseHeaderData) requestFilterResult.header());
            header.setCorrelationId(decodedFrame.correlationId());
            forwardResponse(decodedFrame, header, Objects.requireNonNull(requestFilterResult.message()), null);
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
        CompletableFuture<ApiMessage> p = (CompletableFuture<ApiMessage>) decodedFrame
                .promise();
        boolean newlyCompleted = p.complete(decodedFrame.body());
        if (LOGGER.isDebugEnabled()) {
            if (newlyCompleted) {
                LOGGER.debug("{}: Completed {} response for internal request to filter '{}': {}",
                        channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
            }
            else {
                LOGGER.trace("{}: {} response for internal request to filter '{}' was already completed: {}",
                        channelDescriptor(), decodedFrame.apiKey(), filterDescriptor(), decodedFrame);
            }
        }
    }

    private static <F extends FilterResult> F validateFilterResultNonNull(F f) {
        return Objects.requireNonNullElseGet(f, () -> {
            throw new IllegalStateException("Filter completion must not yield a null result");
        });
    }

    private class InternalFilterContext implements FilterContext {

        private final DecodedFrame<?, ?> decodedFrame;

        @Override
        public Subject authenticatedSubject() {
            return clientSubjectManager.authenticatedSubject();
        }

        InternalFilterContext(DecodedFrame<?, ?> decodedFrame) {
            this.decodedFrame = decodedFrame;
        }

        @Override
        public String channelDescriptor() {
            return FilterHandler.this.channelDescriptor();
        }

        @Override
        public String sessionId() {
            return proxyChannelStateMachine.sessionId();
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
        public Optional<ClientTlsContext> clientTlsContext() {
            return clientSubjectManager.clientTlsContext();
        }

        @Override
        public void clientSaslAuthenticationSuccess(String mechanism,
                                                    String authorizedId) {
            if (deprecationWarningEmitted.compareAndSet(false, true)) {
                LOGGER.warn("Deprecated clientSaslAuthenticationSuccess(String mechanism, String authorizedId) was invoked by filter '{}'. Instead call "
                        + "clientSaslAuthenticationSuccess(String mechanism, Subject subject), ensuring that the Subject contains a {} principal with "
                        + "name equal to authorizedId",
                        filterAndInvoker.filterName(),
                        User.class.getName());
            }
            clientSaslAuthenticationSuccess(mechanism, new Subject(Set.of(new User(authorizedId))));
        }

        @Override
        public void clientSaslAuthenticationSuccess(String mechanism,
                                                    Subject subject) {
            LOGGER.atInfo().setMessage("{}: Filter '{}' announces client has passed SASL authentication using mechanism '{}' and subject '{}'.")
                    .addArgument(channelDescriptor())
                    .addArgument(filterDescriptor())
                    .addArgument(mechanism)
                    .addArgument(subject)
                    .log();
            // dispatch principal injection
            clientSubjectManager.clientSaslAuthenticationSuccess(mechanism, subject);
        }

        @Override
        public void clientSaslAuthenticationFailure(@Nullable String mechanism,
                                                    @Nullable String authorizedId,
                                                    Exception exception) {
            LOGGER.atInfo()
                    .setMessage("{}: Filter '{}' announces client has failed SASL authentication using mechanism '{}' and authorizationId '{}'. Cause message {}."
                            + (LOGGER.isDebugEnabled() ? "" : " Increase log level to DEBUG for stacktrace."))
                    .setCause(LOGGER.isDebugEnabled() ? exception : null)
                    .addArgument(sessionId())
                    .addArgument(filterDescriptor())
                    .addArgument(mechanism)
                    .addArgument(authorizedId)
                    .addArgument(exception.toString())
                    .log();
            clientSubjectManager.clientSaslAuthenticationFailure();
        }

        @Override
        public Optional<ClientSaslContext> clientSaslContext() {
            return clientSubjectManager.clientSaslContext();
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

        @Override
        public <M extends ApiMessage> CompletionStage<M> sendRequest(RequestHeaderData header,
                                                                     ApiMessage request) {
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
            Objects.requireNonNull(ctx).fireChannelRead(frame);
            return filterPromise.minimalCompletionStage();
        }

        @Override
        public CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds) {
            return new TopicNameRetriever(this, Objects.requireNonNull(ctx).executor()).topicNames(topicIds);
        }

    }

}
