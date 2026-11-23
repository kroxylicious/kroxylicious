/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AsyncMapping;
import io.netty.util.concurrent.Future;

import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;

import static io.kroxylicious.proxy.internal.util.NettyFutures.logFailure;

/**
 * Resolves the {@link EndpointBinding} of a TLS client connection from the SNI hostname in the
 * client hello, and then completes the initialisation of the connection's pipeline.
 * <ul>
 * <li>If the resolution succeeds and the binding's gateway provides a downstream {@link SslContext},
 * the connection initializer is invoked (it is expected to add the rest of the pipeline after this
 * handler), this handler is replaced by an {@link io.netty.handler.ssl.SslHandler} built from that
 * context, and {@code channelActive} is fired downstream.</li>
 * <li>Otherwise the channel is closed.</li>
 * </ul>
 * <p>
 * Instances hold per-connection state and must not be shared between channels.
 * </p>
 */
// deep inheritance tree of SniHandler not something we can fix.
@SuppressWarnings("java:S110")
class TlsResolverHandler extends SniHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsResolverHandler.class);

    private final Counter clientToProxyErrorCounter;

    /**
     * Creates a handler that resolves the binding of a TLS client connection.
     * @param ch the client channel whose pipeline this handler is added to
     * @param bindingResolver resolver mapping the accepted channel and SNI hostname to an {@link EndpointBinding}
     * @param connectionInitializer callback, invoked once the binding has resolved, that adds the remaining handlers to the channel's pipeline
     * @param clientToProxyErrorCounter counter incremented when no {@link SslContext} can be found for the connection
     */
    TlsResolverHandler(Channel ch,
                       EndpointBindingResolver bindingResolver,
                       BiConsumer<Channel, EndpointBinding> connectionInitializer,
                       Counter clientToProxyErrorCounter) {
        super(sslContextMapping(Objects.requireNonNull(ch), Objects.requireNonNull(bindingResolver), Objects.requireNonNull(connectionInitializer)));
        this.clientToProxyErrorCounter = Objects.requireNonNull(clientToProxyErrorCounter);
    }

    // Throwable is the right choice as we are forwarding it on.
    @SuppressWarnings("java:S1181")
    private static AsyncMapping<String, SslContext> sslContextMapping(Channel ch,
                                                                      EndpointBindingResolver bindingResolver,
                                                                      BiConsumer<Channel, EndpointBinding> connectionInitializer) {
        return (sniHostname, promise) -> {
            try {
                var stage = bindingResolver.resolve(ch, sniHostname);
                // completes the netty promise when then resolution completes (success/otherwise).
                stage.handle((binding, t) -> {
                    if (Objects.isNull(t)) {
                        try {
                            var gateway = binding.endpointGateway();
                            var sslContext = gateway.getDownstreamSslContext()
                                    .orElseThrow(() -> new IllegalStateException("Virtual cluster %s does not provide SSL context".formatted(gateway)));
                            connectionInitializer.accept(ch, binding);
                            promise.setSuccess(sslContext);
                        }
                        catch (final Throwable t1) {
                            t = t1;
                        }
                    }
                    if (Objects.nonNull(t)) {
                        LOGGER.atWarn()
                                .addKeyValue("channel", ch)
                                .addKeyValue("sniHostname", sniHostname)
                                .addKeyValue("error", t.getMessage())
                                .log("Exception resolving Virtual Cluster Binding");
                        promise.setFailure(t);
                    }
                    return null;
                });
                return promise;
            }
            catch (Throwable cause) {
                return promise.setFailure(cause);
            }
        };
    }

    @Override
    protected void onLookupComplete(ChannelHandlerContext ctx, Future<SslContext> future) throws Exception {
        if (future.isSuccess()) {
            super.onLookupComplete(ctx, future);
            ctx.fireChannelActive();
        }
        else {
            // We've failed to look up the SslContext - this indicates that the SNI hostname was unrecognized
            // or that the virtual cluster is somehow not configured for TLS. All we can do is close the
            // connection.
            clientToProxyErrorCounter.increment();
            ctx.close().addListener(logFailure(LOGGER, "close after SNI/TLS lookup failure"));
        }

    }
}
