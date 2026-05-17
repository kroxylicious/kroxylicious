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

/**
 * Creates {@link TopicPartitionRouter} instances that route Kafka requests
 * to backend clusters based on topic name prefix matching.
 */
@Plugin(configType = TopicPartitionRouterConfig.class)
public class TopicPartitionRouterFactory
        implements RouterFactory<TopicPartitionRouterConfig, TopicPartitionRouterFactory.InitData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRouterFactory.class);

    record InitData(PrefixTopicRoutingTable routingTable,
                    String defaultRoute) {}

    @Override
    public InitData initialize(RouterFactoryContext context,
                               TopicPartitionRouterConfig config)
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

        LOGGER.atInfo()
                .addKeyValue("defaultRoute", config.defaultRoute())
                .addKeyValue("prefixCount", prefixToRoute.size())
                .addKeyValue("routeCount", routingTable.allRoutes().size())
                .log("Topic routing table initialised");

        return new InitData(routingTable, config.defaultRoute());
    }

    @Override
    public Router createRouter(RouterFactoryContext context,
                               InitData initData) {
        return new TopicPartitionRouter(initData.routingTable(), initData.defaultRoute());
    }
}
