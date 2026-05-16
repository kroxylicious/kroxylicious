/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;

import io.kroxylicious.it.testplugins.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;

/**
 * Ensures that basic Virtual-cluster Filter interactions work in combination with a simple
 * passthrough Router.
 */
class FilterRoutedIT extends AbstractFilterIT {

    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "pass-through";
    private static final String CLUSTER_NAME = "backing";

    @Override
    protected ConfigurationBuilder proxyConfig(String bootstrapServers) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(CLUSTER_NAME, null));
        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }
}
