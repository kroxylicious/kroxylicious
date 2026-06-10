/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link RequestDecomposer} contract using a minimal
 * stub implementation that groups {@link ProduceRequestData} topic entries
 * by route.
 */
class RequestDecomposerTest {

    private final TopicRoutingTable table = new TopicRoutingTable() {
        @Override
        public String routeForTopic(String topicName) {
            if (topicName.startsWith("a.")) {
                return "route-a";
            }
            if (topicName.startsWith("b.")) {
                return "route-b";
            }
            return null;
        }

        @Override
        public Set<String> allRoutes() {
            return Set.of("route-a", "route-b");
        }
    };

    private final RequestDecomposer<ProduceRequestData, ProduceResponseData> decomposer = new StubProduceDecomposer();

    @Test
    void shouldDecomposeByRoute() {
        var request = new ProduceRequestData();
        request.topicData().add(topicData("a.orders"));
        request.topicData().add(topicData("b.logs"));
        request.topicData().add(topicData("a.payments"));

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topicData()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topicData()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = new ProduceRequestData();
        request.topicData().add(topicData("a.orders"));
        request.topicData().add(topicData("a.payments"));

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topicData()).hasSize(2);
    }

    @Test
    void shouldRecomposeResponses() {
        var request = new ProduceRequestData();
        request.topicData().add(topicData("a.orders"));
        request.topicData().add(topicData("b.logs"));

        var respA = new ProduceResponseData();
        respA.responses().add(topicResponse("a.orders"));
        var respB = new ProduceResponseData();
        respB.responses().add(topicResponse("b.logs"));

        ProduceResponseData merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request, (short) 0);

        assertThat(merged.responses()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    private static ProduceRequestData.TopicProduceData topicData(String name) {
        return new ProduceRequestData.TopicProduceData().setName(name);
    }

    private static TopicProduceResponse topicResponse(String name) {
        return new TopicProduceResponse().setName(name);
    }

    /**
     * Minimal decomposer for testing the interface contract.
     */
    static class StubProduceDecomposer
            implements RequestDecomposer<ProduceRequestData, ProduceResponseData> {

        @Override
        public Map<String, ProduceRequestData> decompose(
                                                         ProduceRequestData request,
                                                         TopicRoutingTable table,
                                                         short apiVersion) {
            var result = new java.util.LinkedHashMap<String, ProduceRequestData>();
            for (var td : request.topicData()) {
                String route = table.routeForTopic(td.name());
                if (route != null) {
                    result.computeIfAbsent(route, k -> new ProduceRequestData())
                            .topicData().add(td.duplicate());
                }
            }
            return result;
        }

        @Override
        public ProduceResponseData recompose(
                                             Map<String, ProduceResponseData> responses,
                                             ProduceRequestData originalRequest,
                                             short apiVersion) {
            var merged = new ProduceResponseData();
            for (var resp : responses.values()) {
                for (var tr : resp.responses()) {
                    merged.responses().add(tr.duplicate());
                }
            }
            return merged;
        }
    }
}
