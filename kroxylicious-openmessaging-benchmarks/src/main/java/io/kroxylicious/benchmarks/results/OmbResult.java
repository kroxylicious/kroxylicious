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
 * Latency fields use the pre-aggregated scalars that OMB computes from a full-run
 * accumulated HDR histogram — these are the true percentiles over all messages, not
 * a roll-up of per-sample-window values.
 * <p>
 * Throughput fields are per-interval arrays; the mean is returned as the representative value.
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

    @JsonProperty("aggregatedEndToEndLatencyAvg")
    private double endToEndLatencyAvg;

    @JsonProperty("aggregatedEndToEndLatency50pct")
    private double endToEndLatency50pct;

    @JsonProperty("aggregatedEndToEndLatency95pct")
    private double endToEndLatency95pct;

    @JsonProperty("aggregatedEndToEndLatency99pct")
    private double endToEndLatency99pct;

    @JsonProperty("aggregatedEndToEndLatency999pct")
    private double endToEndLatency999pct;

    @JsonProperty("topics")
    private int topics = 1;

    @JsonProperty("producersPerTopic")
    private int producersPerTopic = 1;

    @JsonProperty("consumersPerTopic")
    private int consumersPerTopic = 1;

    @JsonProperty("aggregatedPublishDelayLatencyAvg")
    private double publishDelayLatencyAvgNs;

    @JsonProperty("aggregatedPublishDelayLatency99pct")
    private double publishDelayLatency99pctNs;

    @JsonProperty("publishRate")
    private double[] publishRate;

    @JsonProperty("consumeRate")
    private double[] consumeRate;

    @JsonProperty("publishLatencyAvg")
    private double[] publishLatencyAvgWindows;

    @JsonProperty("publishLatency50pct")
    private double[] publishLatency50pctWindows;

    @JsonProperty("publishLatency95pct")
    private double[] publishLatency95pctWindows;

    @JsonProperty("publishLatency99pct")
    private double[] publishLatency99pctWindows;

    @JsonProperty("publishLatency999pct")
    private double[] publishLatency999pctWindows;

    @JsonProperty("endToEndLatencyAvg")
    private double[] endToEndLatencyAvgWindows;

    @JsonProperty("endToEndLatency50pct")
    private double[] endToEndLatency50pctWindows;

    @JsonProperty("endToEndLatency95pct")
    private double[] endToEndLatency95pctWindows;

    @JsonProperty("endToEndLatency99pct")
    private double[] endToEndLatency99pctWindows;

    @JsonProperty("endToEndLatency999pct")
    private double[] endToEndLatency999pctWindows;

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

    public double getAggregatedEndToEndLatencyAvg() {
        return endToEndLatencyAvg;
    }

    public double getAggregatedEndToEndLatency50pct() {
        return endToEndLatency50pct;
    }

    public double getAggregatedEndToEndLatency95pct() {
        return endToEndLatency95pct;
    }

    public double getAggregatedEndToEndLatency99pct() {
        return endToEndLatency99pct;
    }

    public double getAggregatedEndToEndLatency999pct() {
        return endToEndLatency999pct;
    }

    public double[] getPublishLatencyAvgWindows() {
        return publishLatencyAvgWindows;
    }

    public double[] getPublishLatency50pctWindows() {
        return publishLatency50pctWindows;
    }

    public double[] getPublishLatency95pctWindows() {
        return publishLatency95pctWindows;
    }

    public double[] getPublishLatency99pctWindows() {
        return publishLatency99pctWindows;
    }

    public double[] getPublishLatency999pctWindows() {
        return publishLatency999pctWindows;
    }

    public double[] getEndToEndLatencyAvgWindows() {
        return endToEndLatencyAvgWindows;
    }

    public double[] getEndToEndLatency50pctWindows() {
        return endToEndLatency50pctWindows;
    }

    public double[] getEndToEndLatency95pctWindows() {
        return endToEndLatency95pctWindows;
    }

    public double[] getEndToEndLatency99pctWindows() {
        return endToEndLatency99pctWindows;
    }

    public double[] getEndToEndLatency999pctWindows() {
        return endToEndLatency999pctWindows;
    }

    public double getPublishDelayLatencyAvgNs() {
        return publishDelayLatencyAvgNs;
    }

    public double getPublishDelayLatency99pctNs() {
        return publishDelayLatency99pctNs;
    }

    public double getPublishRate() {
        if (publishRate == null || publishRate.length == 0) {
            return 0.0;
        }
        return Arrays.stream(publishRate).average().orElse(0.0) * topics * producersPerTopic;
    }

    public double[] getPublishRateWindows() {
        if (publishRate == null) {
            return new double[0];
        }
        return Arrays.stream(publishRate).map(r -> r * topics * producersPerTopic).toArray();
    }

    public double getConsumeRate() {
        if (consumeRate == null || consumeRate.length == 0) {
            return 0.0;
        }
        return Arrays.stream(consumeRate).average().orElse(0.0) * topics * consumersPerTopic;
    }

    public double[] getConsumeRateWindows() {
        if (consumeRate == null) {
            return new double[0];
        }
        return Arrays.stream(consumeRate).map(r -> r * topics * consumersPerTopic).toArray();
    }
}
