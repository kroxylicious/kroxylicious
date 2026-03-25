/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the relevant metrics from an OpenMessaging Benchmark result JSON file.
 * <p>
 * Scalar fields correspond to pre-aggregated publish latency values.
 * Array fields contain per-interval measurements that are reduced to a
 * single value using a caller-supplied {@link AggregationMethod}.
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

    @JsonProperty("aggregatedEndToEndLatencyAvg")
    private double aggregatedEndToEndLatencyAvg;

    @JsonProperty("aggregatedEndToEndLatency50pct")
    private double aggregatedEndToEndLatency50pct;

    @JsonProperty("aggregatedEndToEndLatency95pct")
    private double aggregatedEndToEndLatency95pct;

    @JsonProperty("aggregatedEndToEndLatency99pct")
    private double aggregatedEndToEndLatency99pct;

    @JsonProperty("aggregatedEndToEndLatency999pct")
    private double aggregatedEndToEndLatency999pct;

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

    public double getPublishRate(AggregationMethod method) {
        return method.aggregate(publishRate);
    }

    public double getConsumeRate(AggregationMethod method) {
        return method.aggregate(consumeRate);
    }

    public double getAggregatedEndToEndLatencyAvg() {
        return aggregatedEndToEndLatencyAvg;
    }

    public double getAggregatedEndToEndLatency50pct() {
        return aggregatedEndToEndLatency50pct;
    }

    public double getAggregatedEndToEndLatency95pct() {
        return aggregatedEndToEndLatency95pct;
    }

    public double getAggregatedEndToEndLatency99pct() {
        return aggregatedEndToEndLatency99pct;
    }

    public double getAggregatedEndToEndLatency999pct() {
        return aggregatedEndToEndLatency999pct;
    }

    public double getEndToEndLatencyAvg(AggregationMethod method) {
        return method.aggregate(endToEndLatencyAvg);
    }

    public double getEndToEndLatency50pct(AggregationMethod method) {
        return method.aggregate(endToEndLatency50pct);
    }

    public double getEndToEndLatency95pct(AggregationMethod method) {
        return method.aggregate(endToEndLatency95pct);
    }

    public double getEndToEndLatency99pct(AggregationMethod method) {
        return method.aggregate(endToEndLatency99pct);
    }

    public double getEndToEndLatency999pct(AggregationMethod method) {
        return method.aggregate(endToEndLatency999pct);
    }
}
