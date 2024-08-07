/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Simple representation of a Prometheus metric
 * @param name metric name
 * @param labels metric labels
 * @param value metric value
 */
public record SimpleMetric(String name, Map<String, String> labels, double value) {

    // https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
    // note: RE doesn't handle escaping within label values
    @SuppressWarnings("java:S5852") //
    private static final Pattern PROM_TEXT_EXPOSITION_PATTERN = Pattern
            .compile("^(?<metric>[a-zA-Z_:][a-zA-Z0-9_:]*)(\\{(?<labels>.*)})?[\\t ]*(?<value>[0-9E.]*)[\\t ]*(?<timestamp>\\d+)?$");
    private static final Pattern NAME_WITH_QUOTED_VALUE = Pattern.compile("^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)=\"(?<value>.*)\"$");

    static List<SimpleMetric> parse(String output) {
        var all = new ArrayList<SimpleMetric>();
        try (var reader = new BufferedReader(new StringReader(output))) {
            var line = reader.readLine();
            while (line != null) {
                if (!(line.startsWith("#") || line.isEmpty())) {
                    var matched = PROM_TEXT_EXPOSITION_PATTERN.matcher(line);
                    if (!matched.matches()) {
                        throw new IllegalArgumentException("Failed to parse metric %s".formatted(line));
                    }

                    all.add(parseMetric(matched, line));
                }
                line = reader.readLine();
            }
            return all;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to parse metrics", e);
        }
    }

    private static SimpleMetric parseMetric(Matcher matched, String line) {
        try {
            var metricName = matched.group("metric");
            var metricValue = Double.parseDouble(matched.group("value"));
            var metricLabels = matched.group("labels");
            var labels = labelsToMap(metricLabels);
            return new SimpleMetric(metricName, labels, metricValue);
        }
        catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to parse metric %s".formatted(line), iae);
        }
    }

    @NonNull
    private static Map<String, String> labelsToMap(String metricLabels) {
        if (metricLabels == null || metricLabels.isEmpty()) {
            return Map.of();
        }
        var splitLabels = metricLabels.split(",");
        return Arrays.stream(splitLabels)
                .map(NAME_WITH_QUOTED_VALUE::matcher)
                .filter(Matcher::matches)
                .collect(Collectors.toMap(nv -> nv.group("name"), nv -> nv.group("value")));
    }
}
