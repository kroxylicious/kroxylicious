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

    public static void assertMetricValueNullOrZero(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        if (!collector.collectSpecificMetric(pattern).isEmpty()) {
            assertThat(String.format("metric %s doesn't contain 0 value!", pattern),
                    createPatternAndCollectWithoutWait(collector, pattern.toString()).stream().mapToDouble(i -> i).sum(), is(0.0));
        }
    }

    public static void assertMetricValue(MetricsCollector collector, String metric, int expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue,
                is((double) expectedValue));
    }

    public static void assertMetricValueCount(MetricsCollector collector, String metric, long expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", actualValue, expectedValue, metric), actualValue,
                is((double) expectedValue));
    }

    public static void assertMetricCountHigherThan(MetricsCollector collector, String metric, long expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s not is higher than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    public static void assertMetricValueHigherThan(MetricsCollector collector, String metric, int expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    private static List<Double> createPatternAndCollect(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + "\\{.*\\} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.waitForSpecificMetricAndCollect(pattern);
    }

    private static List<Double> createPatternAndCollectWithoutWait(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + "\\{.*\\} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.collectSpecificMetric(pattern);
    }
}
