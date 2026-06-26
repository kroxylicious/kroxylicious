/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResponse;

/**
 * A RouterFactory that captures what it is called with, for use in contract tests.
 * Callers must invoke {@link #reset()} before each test to clear stale state.
 */
@Plugin(configType = RouterContractCapturingFactory.Config.class)
public class RouterContractCapturingFactory
        implements RouterFactory<RouterContractCapturingFactory.Config, RouterContractCapturingFactory.InitData> {

    public record Config(String marker, String route) {}

    public record InitData(
                           Config config,
                           String vcName,
                           String routerName,
                           Set<String> routeNames,
                           Set<String> routerImplNames,
                           boolean allowSharedTargetsSucceeded,
                           boolean pluginInstanceSucceeded) {}

    public record CreateCapture(String vcName, String routerName, InitData initData) {}

    public static final AtomicReference<InitData> capturedInit = new AtomicReference<>();
    public static final AtomicReference<CreateCapture> capturedCreate = new AtomicReference<>();

    public static void reset() {
        capturedInit.set(null);
        capturedCreate.set(null);
    }

    @Override
    public InitData initialize(RouterFactoryContext context, Config config) {
        boolean allowSharedOk = runSilently(context::allowSharedClusterTargets);
        boolean pluginInstanceOk = runSilently(
                () -> context.pluginInstance(RouterFactory.class, RouterContractCapturingFactory.class.getName()));

        var initData = new InitData(
                config,
                context.virtualClusterName(),
                context.routerName(),
                context.routeNames(),
                context.pluginImplementationNames(RouterFactory.class),
                allowSharedOk,
                pluginInstanceOk);
        capturedInit.set(initData);
        return initData;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, InitData initData) {
        capturedCreate.set(new CreateCapture(context.virtualClusterName(), context.routerName(), initData));
        Map<ApiKeys, String> allStatic = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> initData.config().route()));
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey,
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext routerContext) {
                throw new IllegalStateException("Dynamic routing is not supported");
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return allStatic;
            }
        };
    }

    @Override
    public void close(InitData initData) {
    }

    private static boolean runSilently(Runnable r) {
        try {
            r.run();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

}
