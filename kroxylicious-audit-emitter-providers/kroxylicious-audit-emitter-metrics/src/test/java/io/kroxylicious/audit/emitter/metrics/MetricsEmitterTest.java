/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.search.RequiredSearch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricsEmitterTest {

    private SimpleMeterRegistry simpleMeterRegistry;

    @Mock
    AuditableAction mockAction;

    @Mock
    AuditEmitter.Context mockContext;

    @BeforeEach
    void setUp() {
        simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(simpleMeterRegistry);
    }

    @AfterEach
    void tearDown() {
        if (simpleMeterRegistry != null) {
            simpleMeterRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
            Metrics.globalRegistry.remove(simpleMeterRegistry);
        }
    }

    static Stream<Arguments> test() {
        return Stream.of(
                Arguments.argumentSet("Success action with no objectRef",
                        null,
                        Map.of(),
                        "kroxylicious_audit_foo_success",
                        List.of()),
                Arguments.argumentSet("Success action with objectRef keys a subset of tagMapping keys",
                        null,
                        Map.of("vc", "my-cluster"),
                        "kroxylicious_audit_foo_success",
                        List.of(new ImmutableTag("virtual_cluster", "my-cluster"))),
                Arguments.argumentSet("Success action with objectRef keys == tagMapping keys",
                        null,
                        Map.of(
                                "vc", "my-cluster",
                                "topicName", "my-topic"),
                        "kroxylicious_audit_foo_success",
                        List.of(new ImmutableTag("topic_name", "my-topic"),
                                new ImmutableTag("virtual_cluster", "my-cluster"))),
                Arguments.argumentSet("Success action with objectRef keys a superset of tagMapping keys",
                        null,
                        Map.of(
                                "vc", "my-cluster",
                                "topicName", "my-topic",
                                "nodeId", "127.0.0.1"),
                        "kroxylicious_audit_foo_success",
                        List.of(new ImmutableTag("topic_name", "my-topic"),
                                new ImmutableTag("virtual_cluster", "my-cluster"))),
                Arguments.argumentSet("Success action with objectRef keys overlapping tagMapping keys",
                        null,
                        Map.of(
                                "vc", "my-cluster",
                                "nodeId", "127.0.0.1"),
                        "kroxylicious_audit_foo_success",
                        List.of(new ImmutableTag("virtual_cluster", "my-cluster"))),
                Arguments.argumentSet("Failed action",
                        "WhateverException",
                        Map.of(),
                        "kroxylicious_audit_foo_failure",
                        List.of()));
    }

    @ParameterizedTest
    @MethodSource
    void test(@Nullable String status,
              Map<String, String> objectRef,
              String expectedMetricName,
              List<Tag> expectedTags) {
        String action = "foo";

        try (MetricsEmitter metricsEmitter = new MetricsEmitter(Map.of(
                "vc", "virtual_cluster",
                "topicName", "topic_name"))) {

            when(mockAction.action()).thenReturn(action);
            when(mockAction.status()).thenReturn(status);
            when(mockAction.objectRef()).thenReturn(objectRef);

            assertThat(counters(expectedMetricName))
                    .as("Metric should not initially exist")
                    .isEmpty();

            // when
            boolean interested = metricsEmitter.isInterested(action, null);
            metricsEmitter.emitAction(mockAction, mockContext);

            // then
            assertThat(interested).isTrue();

            assertThat(counters(expectedMetricName))
                    .as("Metric should now exist")
                    .singleElement()
                    .isNotNull()
                    .satisfies(counter -> {
                        assertThat(counter.getId().getName()).isEqualTo(expectedMetricName);
                        assertThat(counter.getId().getTags()).isEqualTo(expectedTags);
                        assertThat(counter.count()).isEqualTo(1);
                    });

            // And when
            metricsEmitter.emitAction(mockAction, mockContext);

            // then
            assertThat(counters(expectedMetricName))
                    .as("Metric should still exist")
                    .singleElement()
                    .isNotNull()
                    .satisfies(counter -> {
                        assertThat(counter.getId().getName()).isEqualTo(expectedMetricName);
                        assertThat(counter.getId().getTags()).isEqualTo(expectedTags);
                        assertThat(counter.count()).isEqualTo(2);
                    });
        }

        assertThat(counters(expectedMetricName))
                .as("Metric should not exist after the emitter was closed")
                .isEmpty();
    }

    private static List<Counter> counters(String metricName) {
        RequiredSearch metricSearch = Metrics.globalRegistry.get(metricName);
        List<Counter> list;
        try {
            list = new ArrayList<>(metricSearch.counters());
        }
        catch (MeterNotFoundException e) {
            list = List.of();
        }
        return list;
    }

}