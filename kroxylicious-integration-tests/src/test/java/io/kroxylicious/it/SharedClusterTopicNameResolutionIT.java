/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ConfigurableTopicPrefixFilterFactory;
import io.kroxylicious.it.testplugins.TopicNameResolvingRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that when two routes target the same cluster with different
 * name-transforming filters, {@code TopologyService.topicNames(route, topicIds)}
 * returns the correct per-route name for the same underlying topic UUID.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SharedClusterTopicNameResolutionIT {

    private static final String TOPIC = "shared-topic";
    private static final String PREFIX_A = "a_";
    private static final String PREFIX_B = "b_";
    private static final String CLIENT_ID = "shared-cluster-test";

    KafkaCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        TopicNameResolvingRouterFactory.reset();
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(new NewTopic(TOPIC, Optional.of(1), Optional.empty())))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void tearDown() {
        TopicNameResolvingRouterFactory.reset();
    }

    private ConfigurationBuilder sharedClusterConfig() {
        var clusterDef = new ClusterDefinition("shared-cluster", cluster.getBootstrapServers(), null);

        var filterA = new NamedFilterDefinitionBuilder(
                "prefix-a", ConfigurableTopicPrefixFilterFactory.class.getName())
                .withConfig("prefix", PREFIX_A)
                .build();
        var filterB = new NamedFilterDefinitionBuilder(
                "prefix-b", ConfigurableTopicPrefixFilterFactory.class.getName())
                .withConfig("prefix", PREFIX_B)
                .build();

        var routeA = new RouteDefinition("route-a", 0, List.of("prefix-a"),
                new RouteTarget("shared-cluster", null));
        var routeB = new RouteDefinition("route-b", 1, List.of("prefix-b"),
                new RouteTarget("shared-cluster", null));

        var routerConfig = new TopicNameResolvingRouterFactory.Config(
                "route-a", List.of("route-a", "route-b"));
        var routerDef = new RouterDefinition("resolving-router",
                TopicNameResolvingRouterFactory.class.getName(),
                routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "resolving-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192")
                        .editPortIdentifiesNode()
                        .addToNodeIdRanges(new NamedRange("brokers", 0, 5))
                        .endPortIdentifiesNode()
                        .build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(filterA, filterB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void topicNameResolutionShouldBeRouteScopedForSharedCluster() throws Exception {
        try (var tester = kroxyliciousTester(sharedClusterConfig());
                var client = tester.simpleTestClient()) {

            // Given: negotiate API versions
            client.getSync(new Request(ApiKeys.API_VERSIONS,
                    ApiKeys.API_VERSIONS.latestVersion(),
                    CLIENT_ID,
                    new ApiVersionsRequestData()
                            .setClientSoftwareName("test")
                            .setClientSoftwareVersion("1.0")));

            // Given: send METADATA to warm the topology cache on both routes
            var mdRequest = new MetadataRequestData();
            mdRequest.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(TOPIC));
            var mdRespMsg = client.getSync(new Request(
                    ApiKeys.METADATA, (short) 12, CLIENT_ID, mdRequest));
            var metadataResp = (MetadataResponseData) mdRespMsg.payload().message();

            // Extract the topic UUID from the METADATA response
            Uuid topicId = Uuid.ZERO_UUID;
            for (var topic : metadataResp.topics()) {
                if (topic.name() != null && topic.name().endsWith(TOPIC)) {
                    topicId = topic.topicId();
                    break;
                }
            }
            assertThat(topicId).isNotEqualTo(Uuid.ZERO_UUID);

            // When: send a PRODUCE v13 request with topic ID to trigger resolution
            // (v13+ uses topic IDs instead of names on the wire)
            var produceRequest = new ProduceRequestData()
                    .setAcks((short) -1)
                    .setTimeoutMs(5000);
            produceRequest.topicData().add(new ProduceRequestData.TopicProduceData()
                    .setName("")
                    .setTopicId(topicId));
            client.getSync(new Request(ApiKeys.PRODUCE, (short) 13, CLIENT_ID, produceRequest));
        }

        // Then: the router captured per-route topic name resolutions
        var captures = TopicNameResolvingRouterFactory.drainCaptures();
        assertThat(captures).isNotEmpty();

        var resolved = captures.getFirst();
        Map<String, Map<Uuid, String>> byRoute = resolved.byRoute();

        assertThat(byRoute).containsKeys("route-a", "route-b");

        Map<Uuid, String> routeANames = byRoute.get("route-a");
        Map<Uuid, String> routeBNames = byRoute.get("route-b");

        assertThat(routeANames).hasSize(1);
        assertThat(routeBNames).hasSize(1);

        Uuid topicId = routeANames.keySet().iterator().next();
        assertThat(routeANames.get(topicId)).isEqualTo(PREFIX_A + TOPIC);
        assertThat(routeBNames.get(topicId)).isEqualTo(PREFIX_B + TOPIC);
    }
}
