/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.kroxylicious.kafka.common.message.ListTransactionsRequestData;
import io.kroxylicious.kafka.common.message.ListTransactionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.router.ContextCapturingRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.METADATA;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a router-requested connection close ({@code RouterContext} builder's
 * {@code withCloseConnection()}) does not truncate a response that is still buffered in the
 * response sequencer behind an earlier, still-pending request.
 *
 * <p>This reproduces, end-to-end through a real Netty pipeline, the scenario fixed by
 * {@code 1456a4a7c}: a hard {@code ctx.close()} used to fire before the sequencer drained the
 * buffered out-of-order response, so the client never received it. {@code RoutingHandlerTest}
 * covers the same scenario with an {@code EmbeddedChannel}; this test drives it through a real
 * client connection instead.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingCloseDuringDrainIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "context-capturing";
    private static final String CLUSTER_NAME = "backing";

    @BeforeEach
    void resetRouterState() {
        ContextCapturingRouterFactory.reset();
    }

    // FutureReturnValueIgnored: request 1 (acks=0) is fire-and-forget by design — it exists only
    // to occupy an in-flight slot ahead of `closeResponseFuture`, which is the response under test.
    @SuppressWarnings("FutureReturnValueIgnored")
    @Test
    void closeConnectionResponseIsDeliveredAfterEarlierPendingRequestCompletes() throws Exception {
        // Given: request 1 (PRODUCE, acks=0) is held pending until the test releases it; request 2
        // (LIST_TRANSACTIONS) resolves immediately with closeConnection=true, so its response must
        // be buffered behind request 1's still-open slot in the response sequencer.
        var releaseFirstRequest = new CompletableFuture<Void>();
        var secondRequestReceived = new CompletableFuture<Void>();
        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
            if (apiKey == PRODUCE) {
                return releaseFirstRequest.thenCompose(v -> ctx.respondWithoutReply().completed());
            }
            if (apiKey == LIST_TRANSACTIONS) {
                secondRequestReceived.complete(null);
                return ctx.respondWith(new ListTransactionsResponseData()).withCloseConnection().completed();
            }
            return ctx.respondWith(new MetadataResponseData()).completed();
        });

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> {
            var clusterDef = new ClusterDefinition(CLUSTER_NAME, s, null);
            var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(CLUSTER_NAME, null));
            var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(),
                    new ContextCapturingRouterFactory.Config(ROUTE_NAME), List.of(route));
            var vc = new VirtualClusterBuilder()
                    .withName("demo")
                    .withTarget(new RouteTarget(null, ROUTER_NAME))
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                    .build();
            return baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToRouterDefinitions(routerDef)
                    .addToVirtualClusters(vc);
        }, ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            // Given: the connection is fully established before the race below, so the two
            // requests are guaranteed to be written to the wire in the order they are sent —
            // otherwise both `client.get()` calls race to establish the connection and may be
            // sent in either order.
            client.getSync(new Request(METADATA, (short) 12, "client", new MetadataRequestData()));

            // When
            client.get(new Request(PRODUCE, PRODUCE.latestVersion(), "client", new ProduceRequestData().setAcks((short) 0).setTimeoutMs(5000)));
            var closeResponseFuture = client.get(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));
            secondRequestReceived.get(10, TimeUnit.SECONDS);

            // Then: the close-triggering response is withheld while request 1's slot is still open
            assertThat(closeResponseFuture)
                    .as("close-triggering response must not be delivered ahead of the still-pending earlier request")
                    .isNotDone();

            // When: request 1 is released
            releaseFirstRequest.complete(null);

            // Then: the buffered response is still delivered, and the connection closes afterwards
            assertThat(closeResponseFuture)
                    .as("response for the close-triggering request must be delivered even though it arrived out of sequence")
                    .succeedsWithin(Duration.ofSeconds(10));
            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(client.isOpen()).isFalse());
        }
    }
}
