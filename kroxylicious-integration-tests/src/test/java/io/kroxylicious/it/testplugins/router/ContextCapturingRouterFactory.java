/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins.router;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.EndpointType;
import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Test router factory that captures {@link RouterContext} state on each {@code onRequest} call
 * and delegates to a pluggable action (defaulting to pass-through).
 *
 * <p>Because the proxy runs in-process during integration tests, the static references are
 * visible from both the test code and the router running inside the proxy.</p>
 *
 * <p>Call {@link #reset()} in {@code @BeforeEach} / {@code @AfterEach} to clear state between tests.</p>
 */
@Plugin(configType = ContextCapturingRouterFactory.Config.class)
public class ContextCapturingRouterFactory
        implements RouterFactory<ContextCapturingRouterFactory.Config, ContextCapturingRouterFactory.Config> {

    public record Config(String route) {}

    /**
     * Snapshot of {@link RouterContext} fields captured during the most recent {@code onRequest} call.
     *
     * @param endpoint result of {@link RouterContext#endpoint()}
     * @param nodeForIdZero result of {@link RouterContext#nodeForId(int) nodeForId(0)}
     * @param sessionId result of {@link RouterContext#sessionId()}
     * @param authenticatedSubject result of {@link RouterContext#authenticatedSubject()}
     */
    public record ContextCapture(EndpointType endpoint,
                                 @Nullable VirtualNode nodeForIdZero,
                                 String sessionId,
                                 Subject authenticatedSubject) {}

    /**
     * Action to invoke inside {@code onRequest}. Installed by tests that need custom routing
     * behaviour (e.g. {@code respondWithError}, {@code respondWithoutReply}). If null, the
     * default pass-through is used.
     *
     * <p>The action persists across multiple {@code onRequest} calls until explicitly cleared,
     * so it survives the API_VERSIONS / METADATA requests that precede the test's target request.</p>
     */
    @FunctionalInterface
    public interface OnRequestAction {
        CompletionStage<RouterResponse> act(ApiKeys apiKey,
                                            short apiVersion,
                                            RequestHeaderData header,
                                            ApiMessage request,
                                            RouterContext ctx);
    }

    public static final AtomicReference<ContextCapture> lastCapture = new AtomicReference<>();
    public static final AtomicReference<OnRequestAction> currentAction = new AtomicReference<>();

    public static void reset() {
        lastCapture.set(null);
        currentAction.set(null);
    }

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config config) {
        String route = config.route();
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext ctx) {
                lastCapture.set(new ContextCapture(
                        ctx.endpoint(),
                        safeNodeForId(ctx, 0),
                        ctx.sessionId(),
                        ctx.authenticatedSubject()));

                OnRequestAction action = currentAction.get();
                if (action != null) {
                    return action.act(apiKey, apiVersion, header, request, ctx);
                }
                return ctx.sendToRoute(route, header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
            }

        };
    }

    private static @Nullable VirtualNode safeNodeForId(RouterContext ctx, int virtualNodeId) {
        try {
            return ctx.nodeForId(virtualNodeId);
        }
        catch (Exception e) {
            return null;
        }
    }
}
