/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.RouterContractCapturingFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the contract between the Kroxylicious runtime and RouterFactory implementations:
 * that initialize() is called with the correct deserialized config and a fully-populated
 * RouterFactoryContext, and that createRouter() receives the same initData object that
 * initialize() returned.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RouterFactoryContractIT {

    private static final String VC_NAME = "contract-test-vc";
    private static final String ROUTER_NAME = "contract-capturing";
    private static final String ROUTE_NAME = "backing-route";
    private static final String MARKER = "test-marker-42";

    @BeforeEach
    void resetCapture() {
        RouterContractCapturingFactory.reset();
    }

    @Test
    void shouldHonourRouterFactoryContract(KafkaCluster cluster) throws Exception {
        var routingEnabled = Features.builder().enable(Feature.ROUTING).build();
        try (var tester = KroxyliciousTesters.newBuilder(contractConfig(cluster)).setFeatures(routingEnabled).createDefaultKroxyliciousTester()) {

            // -- initialize() contract --
            // initialize() is called once during proxy startup, before any connections.
            var init = RouterContractCapturingFactory.capturedInit.get();
            assertThat(init).as("initialize() must be called during startup").isNotNull();

            // Config is deserialized to the declared Config record with correct field values.
            assertThat(init.config().marker()).isEqualTo(MARKER);
            assertThat(init.config().route()).isEqualTo(ROUTE_NAME);

            // Context reports the correct virtual-cluster and router identity.
            assertThat(init.vcName()).isEqualTo(VC_NAME);
            assertThat(init.routerName()).isEqualTo(ROUTER_NAME);

            // routeNames() returns the routes declared in the RouterDefinition.
            assertThat(init.routeNames()).containsExactlyInAnyOrder(ROUTE_NAME);

            // pluginImplementationNames() returns a set that includes this factory's own class.
            assertThat(init.routerImplNames())
                    .contains(RouterContractCapturingFactory.class.getName());

            // pluginInstance() resolves a known RouterFactory implementation without throwing.
            assertThat(init.pluginInstanceSucceeded()).isTrue();

            // allowSharedClusterTargets() does not throw.
            assertThat(init.allowSharedTargetsSucceeded()).isTrue();

            // -- createRouter() contract --
            // createRouter() is called per-connection. Trigger one connection via admin.
            try (var admin = tester.admin()) {
                assertThat(admin.listTopics().names())
                        .succeedsWithin(Duration.ofSeconds(10));
            }

            var create = RouterContractCapturingFactory.capturedCreate.get();
            assertThat(create).as("createRouter() must be called when a client connects").isNotNull();

            // Context reports the same virtual-cluster and router identity as initialize().
            assertThat(create.vcName()).isEqualTo(VC_NAME);
            assertThat(create.routerName()).isEqualTo(ROUTER_NAME);

            // The initData passed to createRouter() is exactly the object returned by initialize().
            assertThat(create.initData()).isSameAs(init);
        }
    }

    private ConfigurationBuilder contractConfig(KafkaCluster cluster) {
        var targetCluster = new ClusterDefinition("backing", cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget("backing", null));
        var routerConfig = new RouterContractCapturingFactory.Config(MARKER, ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                RouterContractCapturingFactory.class.getName(), routerConfig, List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName(VC_NAME)
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetCluster)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }
}
