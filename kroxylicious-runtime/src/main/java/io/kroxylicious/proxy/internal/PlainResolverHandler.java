/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.util.NettyFutures.logFailure;

/**
 * Resolves the {@link EndpointBinding} of a plain (non-TLS) client connection and then
 * completes the initialisation of the connection's pipeline.
 * <p>
 * When the channel becomes active the handler asks the {@link EndpointBindingResolver} for the
 * binding. While the resolution is in flight the {@code channelActive} event is withheld and
 * any {@code channelRead} events are buffered.
 * </p>
 * <ul>
 * <li>If the resolution succeeds, the connection initializer is invoked (it is expected to add the
 * rest of the pipeline after this handler), {@code channelActive} is fired downstream, the buffered
 * reads are replayed in arrival order, and the handler removes itself from the pipeline.</li>
 * <li>If the resolution fails, the channel is closed.</li>
 * </ul>
 * <p>
 * Any messages still buffered when the handler is removed from the pipeline are released.
 * </p>
 * <p>
 * Instances hold per-connection state and must not be shared between channels.
 * </p>
 */
class PlainResolverHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlainResolverHandler.class);

    private final EndpointBindingResolver bindingResolver;
    private final BiConsumer<Channel, EndpointBinding> connectionInitializer;
    private final Counter clientToProxyErrorCounter;

    // Buffer channelRead events that arrive before the connection is initialised. This covers
    // two cases: bytes from subsequent TCP segments that arrive before binding resolves
    // (timing race), and leftover bytes fired synchronously by HAProxyMessageDecoder's
    // ByteToMessageDecoder.handlerRemoved when it removes itself after decoding the
    // PROXY frame. The list is only accessed on the event loop thread.
    private final List<Object> pending = new ArrayList<>();

    /**
     * Creates a handler that resolves the binding of a plain client connection.
     * @param bindingResolver resolver mapping the accepted channel to an {@link EndpointBinding}
     * @param connectionInitializer callback, invoked on the channel's event loop once the binding has resolved, that adds the remaining handlers to the channel's pipeline
     * @param clientToProxyErrorCounter counter incremented when the binding cannot be resolved
     */
    PlainResolverHandler(EndpointBindingResolver bindingResolver,
                         BiConsumer<Channel, EndpointBinding> connectionInitializer,
                         Counter clientToProxyErrorCounter) {
        this.bindingResolver = Objects.requireNonNull(bindingResolver);
        this.connectionInitializer = Objects.requireNonNull(connectionInitializer);
        this.clientToProxyErrorCounter = Objects.requireNonNull(clientToProxyErrorCounter);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel ch = ctx.channel();
        bindingResolver.resolve(ch, null)
                .handle((binding, t) -> {
                    ch.eventLoop().execute(() -> onResolutionComplete(ctx, binding, t));
                    return null;
                });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        pending.add(msg);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // Release any messages that were buffered but never replayed (error/close path).
        pending.forEach(ReferenceCountUtil::release);
    }

    // Throwable is the right choice as we are forwarding it on.
    @SuppressWarnings("java:S1181")
    private void onResolutionComplete(ChannelHandlerContext ctx, @Nullable EndpointBinding binding, @Nullable Throwable t) {
        Channel ch = ctx.channel();
        if (t != null) {
            // No virtual cluster for this endpoint (for example, the virtual cluster is being
            // replaced by a reconfigure). Close the connection, as the TLS path does, so the client
            // reconnects at once instead of waiting for its request timeout on a connection that
            // will never be served.
            LOGGER.atWarn()
                    .addKeyValue("channel", ch)
                    .addKeyValue("error", t.getMessage())
                    .setCause(LOGGER.isDebugEnabled() ? t : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "Exception resolving Virtual Cluster Binding, closing connection"
                            : "Exception resolving Virtual Cluster Binding, closing connection. Increase log level to DEBUG for stacktrace");
            clientToProxyErrorCounter.increment();
            ctx.close().addListener(logFailure(LOGGER, "close after binding resolution failure"));
            return;
        }
        try {
            connectionInitializer.accept(ch, binding);
            ctx.fireChannelActive();
            for (Object msg : pending) {
                ctx.fireChannelRead(msg);
            }
            if (!pending.isEmpty()) {
                ctx.fireChannelReadComplete();
            }
            pending.clear();
        }
        catch (Throwable t1) {
            ctx.fireExceptionCaught(t1);
        }
        finally {
            ch.pipeline().remove(this);
        }
    }
}
