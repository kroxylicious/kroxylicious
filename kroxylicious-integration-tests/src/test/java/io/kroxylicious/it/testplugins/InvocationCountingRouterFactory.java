/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A router factory that tracks {@code initialize()} and {@code close()} invocations per
 * config UUID and routes all requests statically to the configured route. Intended for use
 * in hot-reload integration tests that need to assert on router factory lifecycle events.
 */
@Plugin(configType = InvocationCountingRouterFactory.Config.class)
public class InvocationCountingRouterFactory implements RouterFactory<InvocationCountingRouterFactory.Config, InvocationCountingRouterFactory.Config> {

    /**
     * @param configInstanceId unique identifier for this config instance, used to correlate
     *        initialize/close events across hot-reload cycles
     * @param route the route name to which all requests are forwarded statically
     */
    public record Config(UUID configInstanceId, String route) {}

    private static final Map<UUID, AtomicInteger> initializeCounts = new ConcurrentHashMap<>();
    private static final Map<UUID, AtomicInteger> closeCounts = new ConcurrentHashMap<>();

    @Override
    public Config initialize(RouterFactoryContext context, Config config) {
        initializeCounts.computeIfAbsent(config.configInstanceId(), uuid -> new AtomicInteger(0)).incrementAndGet();
        return config;
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Config initData) {
        var allStatic = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> initData.route()));
        return new Router() {
            @Override
            public CompletionStage<RouterResponse> onRequest(ApiKeys apiKey, short apiVersion,
                                                             RequestHeaderData header, ApiMessage request,
                                                             RouterContext routerContext) {
                throw new IllegalStateException("Dynamic routing not supported by InvocationCountingRouterFactory");
            }

            @Override
            public Map<ApiKeys, String> staticRoutes() {
                return allStatic;
            }
        };
    }

    @Override
    public void close(Config initData) {
        closeCounts.computeIfAbsent(initData.configInstanceId(), uuid -> new AtomicInteger(0)).incrementAndGet();
    }

    public static int initializationCountFor(UUID configId) {
        var counter = initializeCounts.get(configId);
        return counter == null ? 0 : counter.get();
    }

    public static int closeCountFor(UUID configId) {
        var counter = closeCounts.get(configId);
        return counter == null ? 0 : counter.get();
    }

    public static void assertAllClosedAndResetCounts() {
        for (var entry : initializeCounts.entrySet()) {
            UUID uuid = entry.getKey();
            assertThat(closeCounts.get(uuid))
                    .as("every initialize() call for %s must have a matching close()", uuid)
                    .hasValue(entry.getValue().intValue());
        }
        assertThat(closeCounts.keySet()).isEqualTo(initializeCounts.keySet());
        initializeCounts.clear();
        closeCounts.clear();
    }
}
