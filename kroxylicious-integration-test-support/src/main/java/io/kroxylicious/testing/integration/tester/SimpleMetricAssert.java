/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.tester;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractDoubleAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.FactoryBasedNavigableListAssert;
import org.assertj.core.api.MapAssert;

import static org.assertj.core.util.Lists.newArrayList;

/**
 * AssertJ assertions for {@link SimpleMetric}s.
 */
public class SimpleMetricAssert extends AbstractAssert<SimpleMetricAssert, SimpleMetric> {

    /**
     * Creates an assertion for a single metric.
     *
     * @param actual the metric under test
     * @return the assertion
     */
    public static SimpleMetricAssert assertThat(SimpleMetric actual) {
        return new SimpleMetricAssert(actual);
    }

    /**
     * Creates an assertion for a list of metrics.
     *
     * @param actual the metrics under test
     * @return the assertion
     */
    public static SimpleMetricListAssert assertThat(List<SimpleMetric> actual) {
        return new SimpleMetricListAssert(actual);
    }

    private SimpleMetricAssert(SimpleMetric simpleMetric) {
        super(simpleMetric, SimpleMetricAssert.class);
        describedAs(simpleMetric == null ? "null metric" : "metric");
    }

    /**
     * Returns an assertion on the metric's value.
     *
     * @return the assertion
     */
    @SuppressWarnings("java:S1452")
    public AbstractDoubleAssert<?> value() {
        isNotNull();
        return Assertions.assertThat(actual.value())
                .describedAs("metric value");
    }

    /**
     * Returns an assertion on the metric's name.
     *
     * @return the assertion
     */
    @SuppressWarnings("java:S1452")
    public AbstractStringAssert<?> name() {
        isNotNull();
        return Assertions.assertThat(actual.name())
                .describedAs("metric name");
    }

    /**
     * Returns an assertion on the metric's labels.
     *
     * @return the assertion
     */
    public MapAssert<String, String> labels() {
        isNotNull();
        return Assertions.assertThat(actual.labels())
                .describedAs("metric labels");
    }

    /**
     * AssertJ assertions for lists of {@link SimpleMetric}s.
     */
    @SuppressWarnings("java:S110") // Ignoring "This class has 6 parents which is greater than 5 authorized" as this is the Assert-J public API.
    public static class SimpleMetricListAssert extends FactoryBasedNavigableListAssert<SimpleMetricListAssert, List<SimpleMetric>, SimpleMetric, SimpleMetricAssert> {
        private SimpleMetricListAssert(List<SimpleMetric> simpleMetrics) {
            super(simpleMetrics, SimpleMetricListAssert.class, SimpleMetricAssert::new);
            var unused = describedAs(simpleMetrics == null ? "empty metric list" : "metrics");

        }

        /**
         * Filters the metrics under test to those with the given name.
         *
         * @param name the metric name to filter on
         * @return an assertion on the filtered metrics
         */
        public SimpleMetricListAssert filterByName(String name) {
            isNotNull();
            return filteredOn(metric -> metric.name().equals(name));
        }

        /**
         * Filters the metrics under test to those with a label of the given name.
         *
         * @param key the label name to filter on
         * @return an assertion on the filtered metrics
         */
        public SimpleMetricListAssert filterByTag(String key) {
            isNotNull();
            return filteredOn(metric -> metric.labels() != null
                    && metric.labels().containsKey(key));
        }

        /**
         * Filters the metrics under test to those with a label of the given name and value.
         *
         * @param key the label name to filter on
         * @param value the label value to filter on
         * @return an assertion on the filtered metrics
         */
        public SimpleMetricListAssert filterByTag(String key, String value) {
            isNotNull();
            return filterByTag(key)
                    .filteredOn(metric -> Objects.equals(metric.labels().get(key), value));
        }

        /**
         * Asserts that exactly one metric matches the given name and tag name/value pairs, and
         * returns an assertion on it.
         *
         * @param name the metric name to match
         * @param tags the tag name/value pairs the metric must carry
         * @return an assertion on the matching metric
         */
        public SimpleMetricAssert withUniqueMetric(String name, Map<String, String> tags) {
            isNotNull();
            Assertions.assertThat(tags).isNotNull();
            return describedAs("no metrics match by name [%s]", name)
                    .filterByName(name)
                    .hasSizeGreaterThan(0)
                    .describedAs("one or more metrics match by name, but none have all of the expected tag names [%s]", String.join(",", tags.keySet()))
                    .filteredOn(sm -> sm.labels().keySet().containsAll(tags.keySet()))
                    .hasSizeGreaterThan(0)
                    .describedAs("one or more metrics match by name, but none have all of the expected tag name/value pairs [%s]",
                            tags.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(
                                    Collectors.joining(",")))
                    .filteredOn(sm -> {
                        var copyTags = new HashMap<>(tags);
                        var copyMetricLabels = new HashMap<>(sm.labels());
                        copyMetricLabels.keySet().retainAll(tags.keySet());
                        return Objects.equals(copyMetricLabels, copyTags);
                    })
                    .singleElement();
        }

        /**
         * Asserts that no metric matches the given name and tag name/value pairs.
         *
         * @param name the metric name to match
         * @param tags the tag name/value pairs to match
         * @return this assertion
         */
        public SimpleMetricListAssert hasNoMetricMatching(String name, Map<String, String> tags) {
            isNotNull();
            Assertions.assertThat(tags).isNotNull();

            var smla = describedAs("found unexpected metric")
                    .filterByName(name);
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                smla = smla.filterByTag(entry.getKey(), entry.getValue());
            }
            smla.isEmpty();
            return this;
        }

        @Override
        protected SimpleMetricListAssert newAbstractIterableAssert(Iterable<? extends SimpleMetric> iterable) {
            return new SimpleMetricListAssert(newArrayList(iterable));
        }
    }
}
