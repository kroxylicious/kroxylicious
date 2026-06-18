/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.topic.config.TopicPartitionRouterConfig;

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
                    Map<String, String> subjectRoutes,
                    ProducerIdManager producerIdManager,
                    FetchSessionCache fetchSessionCache,
                    Clock clock,
                    String virtualClusterName,
                    String routerName) {}

    @Override
    public InitData initialize(RouterFactoryContext context,
                               @NonNull TopicPartitionRouterConfig config)
            throws PluginConfigurationException {
        if ((config.routes() == null || config.routes().isEmpty()) && config.defaultRoute() == null) {
            throw new PluginConfigurationException(
                    "At least one route or a defaultRoute must be configured");
        }

        Map<String, String> prefixToRoute = new LinkedHashMap<>();
        Map<String, String> topicToRoute = new LinkedHashMap<>();
        Map<String, String> subjectRoutes = new LinkedHashMap<>();

        if (config.routes() != null) {
            for (var rc : config.routes()) {
                if (rc.topicPrefixes() != null) {
                    for (var prefix : rc.topicPrefixes()) {
                        String existing = prefixToRoute.put(prefix, rc.name());
                        if (existing != null && !existing.equals(rc.name())) {
                            throw new PluginConfigurationException(
                                    "Prefix '" + prefix + "' is assigned to both route '"
                                            + existing + "' and '" + rc.name() + "'");
                        }
                    }
                }

                if (rc.topics() != null) {
                    for (var topic : rc.topics()) {
                        String existing = topicToRoute.put(topic, rc.name());
                        if (existing != null && !existing.equals(rc.name())) {
                            throw new PluginConfigurationException(
                                    "Topic '" + topic + "' is assigned to both route '"
                                            + existing + "' and '" + rc.name() + "'");
                        }
                    }
                }

                if (rc.subjects() != null) {
                    for (var subject : rc.subjects()) {
                        String existing = subjectRoutes.put(subject, rc.name());
                        if (existing != null && !existing.equals(rc.name())) {
                            throw new PluginConfigurationException(
                                    "Subject '" + subject + "' is assigned to both route '"
                                            + existing + "' and '" + rc.name() + "'");
                        }
                    }
                }
            }
        }

        Set<String> configRouteNames = new LinkedHashSet<>();
        if (config.routes() != null) {
            for (var rc : config.routes()) {
                configRouteNames.add(rc.name());
            }
        }
        if (config.defaultRoute() != null) {
            configRouteNames.add(config.defaultRoute());
        }
        Set<String> unknownRoutes = new LinkedHashSet<>(configRouteNames);
        unknownRoutes.removeAll(context.routeNames());
        if (!unknownRoutes.isEmpty()) {
            throw new PluginConfigurationException(
                    "Route names " + unknownRoutes
                            + " are not defined in the router's routes; available routes: "
                            + context.routeNames());
        }

        PrefixTopicRoutingTable routingTable = PrefixTopicRoutingTable.create(
                prefixToRoute, topicToRoute, config.defaultRoute());

        var ttl = config.producerIdTtl() != null
                ? config.producerIdTtl()
                : ProducerIdManager.DEFAULT_TTL;

        int maxSlots = config.maxFetchSessionCacheSlots() != null
                ? config.maxFetchSessionCacheSlots()
                : FetchSessionCache.DEFAULT_MAX_SLOTS;
        long evictionMs = config.minFetchSessionEviction() != null
                ? config.minFetchSessionEviction().toMillis()
                : FetchSessionCache.DEFAULT_MIN_EVICTION_MS;

        subjectRoutes = Map.copyOf(subjectRoutes);

        LOGGER.atInfo()
                .addKeyValue("defaultRoute", config.defaultRoute())
                .addKeyValue("prefixCount", prefixToRoute.size())
                .addKeyValue("topicCount", topicToRoute.size())
                .addKeyValue("routeCount", routingTable.allRoutes().size())
                .addKeyValue("producerIdTtl", ttl)
                .addKeyValue("maxFetchSessionCacheSlots", maxSlots)
                .addKeyValue("minFetchSessionEvictionMs", evictionMs)
                .addKeyValue("subjectRouteCount", subjectRoutes.size())
                .log("Topic router table initialised");

        Clock clock = clockOverride.get();
        if (clock == null) {
            clock = Clock.systemUTC();
        }

        // Trigger topology cache creation (opt-in side effect).
        // The per-connection TopologyService is obtained in createRouter().
        context.topologyService();

        return new InitData(routingTable, config.defaultRoute(),
                subjectRoutes,
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
                initData.subjectRoutes(),
                initData.producerIdManager(),
                initData.fetchSessionCache(),
                initData.clock(),
                initData.virtualClusterName(),
                initData.routerName(),
                context.topologyService());
    }

    @Override
    public void close(@NonNull InitData initData) {
        initData.producerIdManager().close();
        initData.fetchSessionCache().close();
    }
}
