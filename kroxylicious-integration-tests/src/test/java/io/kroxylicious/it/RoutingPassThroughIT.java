/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TargetClusterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that routing via a pass-through router
 * works identically to the direct target-cluster path.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingPassThroughIT {

    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "pass-through";
    private static final String TARGET_CLUSTER_NAME = "backing";

    private ConfigurationBuilder routingConfig(KafkaCluster cluster) {
        var targetCluster = new TargetClusterDefinition(TARGET_CLUSTER_NAME,
                cluster.getBootstrapServers(), null);

        var route = new RouteDefinition(ROUTE_NAME, null, TARGET_CLUSTER_NAME, null);

        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withRouter(ROUTER_NAME)
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToTargetClusters(targetCluster)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldProduceAndConsumeViaPassThroughRouter(KafkaCluster cluster, Topic topic) throws Exception {
        var config = routingConfig(cluster);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            producer.send(new ProducerRecord<>(topic.name(), "key", "value"))
                    .get(10, TimeUnit.SECONDS);

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            var record = records.iterator().next();
            assertThat(record.key()).isEqualTo("key");
            assertThat(record.value()).isEqualTo("value");
        }
    }

    @Test
    void shouldListTopicsViaPassThroughRouter(KafkaCluster cluster, Topic topic) throws Exception {
        var config = routingConfig(cluster);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin()) {

            var topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(topics).contains(topic.name());
        }
    }
}
