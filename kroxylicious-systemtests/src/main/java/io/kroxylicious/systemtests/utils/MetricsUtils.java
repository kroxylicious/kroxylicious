/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.List;
import java.util.regex.Pattern;

import io.kroxylicious.systemtests.metrics.MetricsCollector;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *  Provides auxiliary methods for Scraper Pod, which reaches Kroxylicious in the Kubernetes cluster.
 */
public class MetricsUtils {

    public static final String MATCH_LABELS_PATTERN = "\\{.*}";
    public static final String MATCH_VALUE_PATTERN = "\\s*([\\d.^\\n]+)";

    private MetricsUtils() {
    }

    public static void assertMetricResourceNotNull(MetricsCollector collector, String metric, String kind) {
        String metrics = metric + "\\{kind=\"" + kind + "\",.*}";
        assertMetricValueNotNull(collector, metrics);
    }

    public static void assertMetricValueNotNull(MetricsCollector collector, String metric) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' doesn't exist", metric), actualValue, notNullValue());
    }

    public static void assertMetricValue(MetricsCollector collector, String metric, int expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(
                String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue),
                actualValue,
                is((double) expectedValue)
        );
    }

    public static void assertMetricValueCount(MetricsCollector collector, String metric, long expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualCount = values.stream().mapToDouble(i -> i).count();
        assertThat(
                String.format("metric '%s' count %s is different than expected %s", actualCount, expectedValue, metric),
                actualCount,
                is((double) expectedValue)
        );
    }

    public static void assertMetricCountHigherThan(MetricsCollector collector, String metric, long expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual count value %s is not higher than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    public static void assertMetricValueHigherThan(MetricsCollector collector, String metric, int expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is not higher than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    private static List<Double> createPatternAndCollect(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + MATCH_LABELS_PATTERN + MATCH_VALUE_PATTERN, Pattern.CASE_INSENSITIVE);
        return collector.waitForSpecificMetricAndCollect(pattern);
    }

}
