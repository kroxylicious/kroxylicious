/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.LinkedHashMap;
import java.util.Map;

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

    record InitData(PrefixTopicRoutingTable routingTable,
                    @Nullable String defaultRoute,
                    ProducerIdManager producerIdManager,
                    FetchSessionCache fetchSessionCache) {}

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

        LOGGER.atInfo()
                .addKeyValue("defaultRoute", config.defaultRoute())
                .addKeyValue("prefixCount", prefixToRoute.size())
                .addKeyValue("routeCount", routingTable.allRoutes().size())
                .addKeyValue("producerIdTtl", ttl)
                .addKeyValue("maxFetchSessionCacheSlots", maxSlots)
                .addKeyValue("minFetchSessionEvictionMs", evictionMs)
                .log("Topic routing table initialised");

        return new InitData(routingTable, config.defaultRoute(),
                new ProducerIdManager(ttl),
                new FetchSessionCache(maxSlots, evictionMs,
                        context.virtualClusterName(), context.routerName()));
    }

    @Override
    public Router createRouter(RouterFactoryContext context,
                               @NonNull InitData initData) {
        return new TopicPartitionRouter(
                initData.routingTable(),
                initData.defaultRoute(),
                initData.producerIdManager(),
                initData.fetchSessionCache());
    }

    @Override
    public void close(@NonNull InitData initData) {
        initData.producerIdManager().close();
    }
}
