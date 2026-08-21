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
 * <b>Latency scalars</b> ({@code aggregated*} JSON fields) are pre-aggregated by OMB from
 * a full-run accumulated HDR histogram — these are the true percentiles over all messages,
 * not a roll-up of per-sample-window values. Units are milliseconds.
 * <p>
 * <b>Throughput arrays</b> ({@code publishRate}, {@code consumeRate}) contain one entry per
 * sample window. Each element is the mean rate for that window in msgs/sec
 * <em>per producer (or consumer) per topic</em> — i.e. a single logical producer's rate,
 * not the cluster-wide total. To get the total publish rate across all producers and topics,
 * multiply by {@code topics × producersPerTopic}; this is what {@link #getPublishRate()} returns.
 * <p>
 * <b>Publish delay</b> ({@code aggregatedPublishDelayLatency*} JSON fields) measures the time
 * producers spent blocked waiting to enqueue a message. Values above ~1ms indicate producers
 * are back-pressured by the broker. Units are nanoseconds.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OmbResult {

    /**
     * Creates an empty result; fields are populated by Jackson during deserialization.
     */
    public OmbResult() {
        // empty
    }

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

    /**
     * Gets the average publish latency over the full run.
     *
     * @return average publish latency in milliseconds
     */
    public double getPublishLatencyAvg() {
        return publishLatencyAvg;
    }

    /**
     * Gets the 50th percentile publish latency over the full run.
     *
     * @return p50 publish latency in milliseconds
     */
    public double getPublishLatency50pct() {
        return publishLatency50pct;
    }

    /**
     * Gets the 95th percentile publish latency over the full run.
     *
     * @return p95 publish latency in milliseconds
     */
    public double getPublishLatency95pct() {
        return publishLatency95pct;
    }

    /**
     * Gets the 99th percentile publish latency over the full run.
     *
     * @return p99 publish latency in milliseconds
     */
    public double getPublishLatency99pct() {
        return publishLatency99pct;
    }

    /**
     * Gets the 99.9th percentile publish latency over the full run.
     *
     * @return p99.9 publish latency in milliseconds
     */
    public double getPublishLatency999pct() {
        return publishLatency999pct;
    }

    /**
     * Gets the average end-to-end latency over the full run.
     *
     * @return average end-to-end latency in milliseconds
     */
    public double getAggregatedEndToEndLatencyAvg() {
        return endToEndLatencyAvg;
    }

    /**
     * Gets the 50th percentile end-to-end latency over the full run.
     *
     * @return p50 end-to-end latency in milliseconds
     */
    public double getAggregatedEndToEndLatency50pct() {
        return endToEndLatency50pct;
    }

    /**
     * Gets the 95th percentile end-to-end latency over the full run.
     *
     * @return p95 end-to-end latency in milliseconds
     */
    public double getAggregatedEndToEndLatency95pct() {
        return endToEndLatency95pct;
    }

    /**
     * Gets the 99th percentile end-to-end latency over the full run.
     *
     * @return p99 end-to-end latency in milliseconds
     */
    public double getAggregatedEndToEndLatency99pct() {
        return endToEndLatency99pct;
    }

    /**
     * Gets the 99.9th percentile end-to-end latency over the full run.
     *
     * @return p99.9 end-to-end latency in milliseconds
     */
    public double getAggregatedEndToEndLatency999pct() {
        return endToEndLatency999pct;
    }

    /**
     * Gets the per-sample-window average publish latencies.
     *
     * @return average publish latency per window in milliseconds
     */
    public double[] getPublishLatencyAvgWindows() {
        return publishLatencyAvgWindows;
    }

    /**
     * Gets the per-sample-window 50th percentile publish latencies.
     *
     * @return p50 publish latency per window in milliseconds
     */
    public double[] getPublishLatency50pctWindows() {
        return publishLatency50pctWindows;
    }

    /**
     * Gets the per-sample-window 95th percentile publish latencies.
     *
     * @return p95 publish latency per window in milliseconds
     */
    public double[] getPublishLatency95pctWindows() {
        return publishLatency95pctWindows;
    }

    /**
     * Gets the per-sample-window 99th percentile publish latencies.
     *
     * @return p99 publish latency per window in milliseconds
     */
    public double[] getPublishLatency99pctWindows() {
        return publishLatency99pctWindows;
    }

    /**
     * Gets the per-sample-window 99.9th percentile publish latencies.
     *
     * @return p99.9 publish latency per window in milliseconds
     */
    public double[] getPublishLatency999pctWindows() {
        return publishLatency999pctWindows;
    }

    /**
     * Gets the per-sample-window average end-to-end latencies.
     *
     * @return average end-to-end latency per window in milliseconds
     */
    public double[] getEndToEndLatencyAvgWindows() {
        return endToEndLatencyAvgWindows;
    }

    /**
     * Gets the per-sample-window 50th percentile end-to-end latencies.
     *
     * @return p50 end-to-end latency per window in milliseconds
     */
    public double[] getEndToEndLatency50pctWindows() {
        return endToEndLatency50pctWindows;
    }

    /**
     * Gets the per-sample-window 95th percentile end-to-end latencies.
     *
     * @return p95 end-to-end latency per window in milliseconds
     */
    public double[] getEndToEndLatency95pctWindows() {
        return endToEndLatency95pctWindows;
    }

    /**
     * Gets the per-sample-window 99th percentile end-to-end latencies.
     *
     * @return p99 end-to-end latency per window in milliseconds
     */
    public double[] getEndToEndLatency99pctWindows() {
        return endToEndLatency99pctWindows;
    }

    /**
     * Gets the per-sample-window 99.9th percentile end-to-end latencies.
     *
     * @return p99.9 end-to-end latency per window in milliseconds
     */
    public double[] getEndToEndLatency999pctWindows() {
        return endToEndLatency999pctWindows;
    }

    /**
     * Gets the average time producers spent blocked waiting to enqueue a message.
     * Values above ~1ms indicate producers are back-pressured by the broker.
     *
     * @return average publish delay in nanoseconds
     */
    public double getPublishDelayLatencyAvgNs() {
        return publishDelayLatencyAvgNs;
    }

    /**
     * Gets the 99th percentile of the time producers spent blocked waiting to enqueue a message.
     *
     * @return p99 publish delay in nanoseconds
     */
    public double getPublishDelayLatency99pctNs() {
        return publishDelayLatency99pctNs;
    }

    /**
     * Gets the mean publish rate over the full run, totalled across all producers and topics.
     *
     * @return total publish rate in msgs/sec, or {@code 0.0} if no rate data is present
     */
    public double getPublishRate() {
        if (publishRate == null || publishRate.length == 0) {
            return 0.0;
        }
        return Arrays.stream(publishRate).average().orElse(0.0) * topics * producersPerTopic;
    }

    /**
     * Gets the per-sample-window publish rates, totalled across all producers and topics.
     *
     * @return total publish rate per window in msgs/sec, or an empty array if no rate data is present
     */
    public double[] getPublishRateWindows() {
        if (publishRate == null) {
            return new double[0];
        }
        return Arrays.stream(publishRate).map(r -> r * topics * producersPerTopic).toArray();
    }

    /**
     * Gets the mean consume rate over the full run, totalled across all consumers and topics.
     *
     * @return total consume rate in msgs/sec, or {@code 0.0} if no rate data is present
     */
    public double getConsumeRate() {
        if (consumeRate == null || consumeRate.length == 0) {
            return 0.0;
        }
        return Arrays.stream(consumeRate).average().orElse(0.0) * topics * consumersPerTopic;
    }

    /**
     * Gets the per-sample-window consume rates, totalled across all consumers and topics.
     *
     * @return total consume rate per window in msgs/sec, or an empty array if no rate data is present
     */
    public double[] getConsumeRateWindows() {
        if (consumeRate == null) {
            return new double[0];
        }
        return Arrays.stream(consumeRate).map(r -> r * topics * consumersPerTopic).toArray();
    }
}
