/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;

/**
 * Sits at the end of the VC-level filter chain (instead of
 * {@link io.kroxylicious.proxy.internal.FilterChainCompletionHandler}) when a
 * virtual cluster uses a router. Forwards statically-routed requests
 * directly to the {@link ClientConnectionStateMachine}; rejects any
 * request whose API key is not covered by the static routes.
 * <p>
 * Also intercepts decoded responses to apply node ID translation using
 * the supplied {@link NodeIdMapping}, and caches upstream broker addresses
 * from METADATA responses to enable per-broker connections.
 */
public class RouterDispatchHandler extends ChannelDuplexHandler {

    /**
     * API keys whose responses carry node IDs that must be translated to virtual node IDs.
     * These keys are always decoded (even when statically routed) so the response bodies
     * are accessible for translation.
     */
    public static final Set<ApiKeys> NODE_ID_TRANSLATION_APIS = Set.of(
            ApiKeys.METADATA,
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.DESCRIBE_CLUSTER,
            ApiKeys.PRODUCE,
            ApiKeys.FETCH,
            ApiKeys.SHARE_FETCH,
            ApiKeys.SHARE_ACKNOWLEDGE,
            ApiKeys.DESCRIBE_TOPIC_PARTITIONS);

    private final Router router;
    private final Map<ApiKeys, String> staticRoutes;
    private final ClientConnectionStateMachine ccsm;
    private final NodeIdMapping nodeIdMapping;

    /**
     * Tracks correlation IDs of in-flight requests that need response translation.
     * Entries are removed when the response arrives in {@link #write}. Any entries
     * remaining at channel close are discarded with the handler — no explicit cleanup
     * is required.
     */
    private final Map<Integer, String> pendingRoutes = new HashMap<>();

    public RouterDispatchHandler(Router router,
                                 Map<ApiKeys, String> staticRoutes,
                                 ClientConnectionStateMachine ccsm,
                                 NodeIdMapping nodeIdMapping) {
        this.router = router;
        this.staticRoutes = staticRoutes;
        this.ccsm = ccsm;
        this.nodeIdMapping = nodeIdMapping;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        router.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestFrame frame) {
            ApiKeys apiKey = ApiKeys.forId(frame.apiKeyId());
            String staticRoute = staticRoutes.get(apiKey);
            if (staticRoute != null) {
                if (NODE_ID_TRANSLATION_APIS.contains(apiKey)) {
                    pendingRoutes.put(frame.correlationId(), staticRoute);
                }
                ccsm.forwardToRoute(staticRoute, msg);
                return;
            }
            throw new IllegalStateException(
                    "Dynamic routing is not supported. API key " + apiKey + " is not covered by staticRoutes().");
        }
        throw new IllegalStateException(
                "Unexpected non-frame message in routing pipeline: " + msg.getClass().getName());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DecodedResponseFrame<?> frame) {
            String routeName = pendingRoutes.remove(frame.correlationId());
            if (routeName != null) {
                NodeIdResponseTranslator.translate(frame.body(), frame.apiVersion(), nodeIdMapping, routeName);
            }
        }
        ctx.write(msg, promise);
    }
}
