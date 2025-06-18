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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void uniqueMetricCanExtractByMatchingMetricNameAndTag() {
        var metric1 = new SimpleMetric("foo", Map.of("animal", "capybara"), 1);
        var metric2 = new SimpleMetric("foo", Map.of("animal", "squirrel"), 2);

        assertThat(List.of(metric1, metric2))
                .withUniqueMetric("foo", Map.of("animal", "capybara"))
                .isEqualTo(metric1);
    }

    @Test
    void uniqueMetricIgnoresOtherTags() {
        var metric = new SimpleMetric("foo", Map.of("animal", "capybara", "domain", "america"), 1);

        assertThat(List.of(metric))
                .withUniqueMetric("foo", Map.of("animal", "capybara"))
                .isEqualTo(metric);
    }

    @Test
    void uniqueMetricSupportsEmptyDesiredTags() {
        var metric = new SimpleMetric("foo", Map.of("animal", "capybara", "domain", "america"), 1);

        assertThat(List.of(metric))
                .withUniqueMetric("foo", Map.of())
                .isEqualTo(metric);
    }

    @Test
    void uniqueMetricShouldFailOnMismatchedMetricName() {
        var metrics = List.of(new SimpleMetric("foo", Map.of(), 1));

        var desiredTags = Map.of("a", "b");
        var listAssert = assertThat(metrics);
        assertThatThrownBy(() -> listAssert.withUniqueMetric("bar", desiredTags))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("no metrics match by name [bar]");
    }

    @Test
    void uniqueMetricShouldFailOnMismatchedMetricTagName() {
        var metrics = List.of(new SimpleMetric("foo", Map.of("a", "b"), 1));

        var desiredTags = Map.of("A", "B");
        var listAssert = assertThat(metrics);
        assertThatThrownBy(() -> listAssert.withUniqueMetric("foo", desiredTags))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("one or more metrics match by name, but none have all of the expected tag names [A]");
    }

    @Test
    void uniqueMetricShouldFailOnMismatchedMetricTagNameValue() {
        var metrics = List.of(new SimpleMetric("foo", Map.of("a", "b"), 1));

        var desiredTags = Map.of("a", "B");
        var listAssert = assertThat(metrics);
        assertThatThrownBy(() -> listAssert.withUniqueMetric("foo", desiredTags))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("one or more metrics match by name, but none have all of the expected tag name/value pairs [a=B]");
    }

    @Test
    void hasNoMetricMatchingShouldNotFailIfNameAndTagMatches() {
        var metric1 = new SimpleMetric("foo", Map.of("animal", "capybara"), 1);
        var metric2 = new SimpleMetric("foo", Map.of("animal", "squirrel"), 2);

        assertThat(List.of(metric1, metric2))
                .hasNoMetricMatching("foo", Map.of("animal", "aardvark"))
                .hasSize(2);
    }

    @Test
    void hasNoMetricMatchingShouldFailOnUnmatchedMetricName() {
        var metrics = List.of(new SimpleMetric("foo", Map.of(), 1));

        var emptyTag = Map.<String, String> of();
        var listAssert = assertThat(metrics);
        assertThatThrownBy(() -> listAssert.hasNoMetricMatching("foo", emptyTag))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("found unexpected metric");
    }

    @Test
    void hasNoMetricMatchingShouldFailOnUnmatchedMetricTag() {
        var metrics = List.of(new SimpleMetric("foo", Map.of("a", "b"), 1));

        var desiredTags = Map.of("a", "b");
        var listAssert = assertThat(metrics);
        assertThatThrownBy(() -> listAssert.hasNoMetricMatching("foo", desiredTags))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("found unexpected metric");
    }

    @Test
    void hasNoMetricMatchingShouldIgnoreAdditionalTags() {
        var metrics = List.of(new SimpleMetric("foo", Map.of("a", "b", "e", "f"), 1));

        var desiredTags = Map.of("a", "b");
        var listAssert = assertThat(metrics);
        assertThatThrownBy(() -> listAssert.hasNoMetricMatching("foo", desiredTags))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("found unexpected metric");
    }

}