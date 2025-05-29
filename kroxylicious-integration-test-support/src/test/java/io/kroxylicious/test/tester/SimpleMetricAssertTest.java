/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static io.kroxylicious.test.tester.SimpleMetricAssert.assertThat;

class SimpleMetricAssertTest {

    @Test
    void canAssertMetricName() {
        var metric = new SimpleMetric("foo", Map.of(), 5);
        var simpleMetricAssert = assertThat(metric);
        simpleMetricAssert.name().isEqualTo("foo");
    }

    @Test
    void canAssertMetricValue() {
        var metric = new SimpleMetric("foo", Map.of(), 5);
        var simpleMetricAssert = assertThat(metric);
        simpleMetricAssert.value().isEqualTo(5.0);
    }

    @Test
    void canAssertMetricLabel() {
        var metric = new SimpleMetric("foo", Map.of("fruit", "banana", "animal", "capybara"), 5);
        var simpleMetricAssert = assertThat(metric);
        simpleMetricAssert.labels()
                .containsKey("animal")
                .containsEntry("animal", "capybara");
    }

    @Test
    void canFilterByMetricName() {
        var metric1 = new SimpleMetric("foo", Map.of(), 1);
        var metric2 = new SimpleMetric("bar", Map.of(), 1);
        assertThat(List.of(metric1, metric2))
                .hasSize(2)
                .filterByName("foo")
                .hasSize(1)
                .singleElement()
                .name()
                .isEqualTo("foo");
    }

    @Test
    void canFilterByTag() {
        var metric1 = new SimpleMetric("foo", Map.of("animal", "capybara"), 1);
        var metric2 = new SimpleMetric("bar", Map.of("animal", "squirrel"), 1);
        var metric3 = new SimpleMetric("baz", Map.of("planet", "venus"), 1);
        assertThat(List.of(metric1, metric2, metric3))
                .hasSize(3)
                .filterByTag("animal")
                .hasSize(2)
                .satisfiesExactly(e1 -> assertThat(e1).name().isEqualTo("foo"),
                        e2 -> assertThat(e2).name().isEqualTo("bar"));
    }

    @Test
    void canFilterByTagValue() {
        var metric1 = new SimpleMetric("foo", Map.of("animal", "capybara"), 1);
        var metric2 = new SimpleMetric("bar", Map.of("animal", "squirrel"), 1);
        assertThat(List.of(metric1, metric2))
                .hasSize(2)
                .filterByTag("animal", "squirrel")
                .hasSize(1)
                .singleElement()
                .name()
                .isEqualTo("bar");
    }

}