/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.routing.Router;
import io.kroxylicious.proxy.routing.RouterFactory;
import io.kroxylicious.proxy.routing.RouterFactoryContext;
import io.kroxylicious.proxy.routing.topic.config.TopicPartitionRouterConfig;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Creates {@link TopicPartitionRouter} instances that route Kafka requests
 * to backend clusters based on topic name prefix matching.
 */
@Plugin(configType = TopicPartitionRouterConfig.class)
public class TopicPartitionRouterFactory
        implements RouterFactory<TopicPartitionRouterConfig, TopicPartitionRouterFactory.InitData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRouterFactory.class);

    private static final AtomicReference<Clock> clockOverride = new AtomicReference<>();

    static void setClockOverride(Clock clock) {
        clockOverride.set(clock);
    }

    static void clearClockOverride() {
        clockOverride.set(null);
    }

    record InitData(PrefixTopicRoutingTable routingTable,
                    @Nullable String defaultRoute,
                    Map<String, String> transactionalUserRoutes,
                    Map<String, String> consumerGroupUserRoutes,
                    ProducerIdManager producerIdManager,
                    FetchSessionCache fetchSessionCache,
                    Clock clock,
                    String virtualClusterName,
                    String routerName) {}

    @Override
    public InitData initialize(RouterFactoryContext context,
                               @NonNull TopicPartitionRouterConfig config)
            throws PluginConfigurationException {
        if (config.topicRoutes() == null || config.topicRoutes().isEmpty()) {
            if (config.defaultRoute() == null) {
                throw new PluginConfigurationException(
                        "At least one topicRoute or a defaultRoute must be configured");
            }
        }

        Map<String, String> prefixToRoute = new LinkedHashMap<>();
        if (config.topicRoutes() != null) {
            for (var tr : config.topicRoutes()) {
                for (var prefix : tr.topicPrefixes()) {
                    String existing = prefixToRoute.put(prefix, tr.route());
                    if (existing != null && !existing.equals(tr.route())) {
                        throw new PluginConfigurationException(
                                "Prefix '" + prefix + "' is assigned to both route '"
                                        + existing + "' and '" + tr.route() + "'");
                    }
                }
            }
        }

        PrefixTopicRoutingTable routingTable = PrefixTopicRoutingTable.create(
                prefixToRoute, config.defaultRoute());

        var ttl = config.producerIdTtl() != null
                ? config.producerIdTtl()
                : ProducerIdManager.DEFAULT_TTL;

        int maxSlots = config.maxFetchSessionCacheSlots() != null
                ? config.maxFetchSessionCacheSlots()
                : FetchSessionCache.DEFAULT_MAX_SLOTS;
        long evictionMs = config.minFetchSessionEviction() != null
                ? config.minFetchSessionEviction().toMillis()
                : FetchSessionCache.DEFAULT_MIN_EVICTION_MS;

        Map<String, String> txnUserRoutes = config.transactionalUserRoutes() != null
                ? Map.copyOf(config.transactionalUserRoutes())
                : Map.of();

        Map<String, String> cgUserRoutes = config.consumerGroupUserRoutes() != null
                ? Map.copyOf(config.consumerGroupUserRoutes())
                : Map.of();

        Set<String> allRouteNames = routingTable.allRoutes();
        for (var entry : txnUserRoutes.entrySet()) {
            if (!allRouteNames.contains(entry.getValue())) {
                throw new PluginConfigurationException(
                        "transactionalUserRoutes maps user '" + entry.getKey()
                                + "' to unknown route '" + entry.getValue() + "'");
            }
        }
        for (var entry : cgUserRoutes.entrySet()) {
            if (!allRouteNames.contains(entry.getValue())) {
                throw new PluginConfigurationException(
                        "consumerGroupUserRoutes maps user '" + entry.getKey()
                                + "' to unknown route '" + entry.getValue() + "'");
            }
        }

        LOGGER.atInfo()
                .addKeyValue("defaultRoute", config.defaultRoute())
                .addKeyValue("prefixCount", prefixToRoute.size())
                .addKeyValue("routeCount", allRouteNames.size())
                .addKeyValue("producerIdTtl", ttl)
                .addKeyValue("maxFetchSessionCacheSlots", maxSlots)
                .addKeyValue("minFetchSessionEvictionMs", evictionMs)
                .addKeyValue("transactionalUserRouteCount", txnUserRoutes.size())
                .addKeyValue("consumerGroupUserRouteCount", cgUserRoutes.size())
                .log("Topic routing table initialised");

        Clock clock = clockOverride.get();
        if (clock == null) {
            clock = Clock.systemUTC();
        }

        return new InitData(routingTable, config.defaultRoute(),
                txnUserRoutes,
                cgUserRoutes,
                new ProducerIdManager(ttl),
                new FetchSessionCache(maxSlots, evictionMs,
                        context.virtualClusterName(), context.routerName()),
                clock,
                context.virtualClusterName(),
                context.routerName());
    }

    @Override
    public Router createRouter(RouterFactoryContext context,
                               @NonNull InitData initData) {
        return new TopicPartitionRouter(
                initData.routingTable(),
                initData.defaultRoute(),
                initData.transactionalUserRoutes(),
                initData.consumerGroupUserRoutes(),
                initData.producerIdManager(),
                initData.fetchSessionCache(),
                initData.clock(),
                initData.virtualClusterName(),
                initData.routerName());
    }

    @Override
    public void close(@NonNull InitData initData) {
        initData.producerIdManager().close();
        initData.fetchSessionCache().close();
    }
}
