/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the relevant metrics from an OpenMessaging Benchmark result JSON file.
 * <p>
 * Scalar fields correspond to pre-aggregated publish latency values.
 * Array fields contain per-interval measurements that should be averaged
 * for comparison purposes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OmbResult {

    @JsonProperty("aggregatedPublishLatencyAvg")
    private double publishLatencyAvg;

    @JsonProperty("aggregatedPublishLatency50pct")
    private double publishLatency50pct;

    @JsonProperty("aggregatedPublishLatency95pct")
    private double publishLatency95pct;

    @JsonProperty("aggregatedPublishLatency99pct")
    private double publishLatency99pct;

    @JsonProperty("aggregatedPublishLatency999pct")
    private double publishLatency999pct;

    @JsonProperty("publishRate")
    private double[] publishRate;

    @JsonProperty("consumeRate")
    private double[] consumeRate;

    @JsonProperty("endToEndLatencyAvg")
    private double[] endToEndLatencyAvg;

    @JsonProperty("endToEndLatency50pct")
    private double[] endToEndLatency50pct;

    @JsonProperty("endToEndLatency95pct")
    private double[] endToEndLatency95pct;

    @JsonProperty("endToEndLatency99pct")
    private double[] endToEndLatency99pct;

    @JsonProperty("endToEndLatency999pct")
    private double[] endToEndLatency999pct;

    /**
     * Computes the arithmetic mean of the given array.
     *
     * @param values the values to average
     * @return the mean, or 0.0 if the array is null or empty
     */
    public static double arrayAverage(double[] values) {
        if (values == null || values.length == 0) {
            return 0.0;
        }
        return Arrays.stream(values).average().orElse(0.0);
    }

    public double getPublishLatencyAvg() {
        return publishLatencyAvg;
    }

    public double getPublishLatency50pct() {
        return publishLatency50pct;
    }

    public double getPublishLatency95pct() {
        return publishLatency95pct;
    }

    public double getPublishLatency99pct() {
        return publishLatency99pct;
    }

    public double getPublishLatency999pct() {
        return publishLatency999pct;
    }

    public double[] getPublishRate() {
        return publishRate;
    }

    public double[] getConsumeRate() {
        return consumeRate;
    }

    public double[] getEndToEndLatencyAvg() {
        return endToEndLatencyAvg;
    }

    public double[] getEndToEndLatency50pct() {
        return endToEndLatency50pct;
    }

    public double[] getEndToEndLatency95pct() {
        return endToEndLatency95pct;
    }

    public double[] getEndToEndLatency99pct() {
        return endToEndLatency99pct;
    }

    public double[] getEndToEndLatency999pct() {
        return endToEndLatency999pct;
    }
}
