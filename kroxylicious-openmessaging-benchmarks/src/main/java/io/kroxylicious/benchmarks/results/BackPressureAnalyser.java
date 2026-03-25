/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.List;

/**
 * Analyses a set of labelled OMB results for producer back-pressure.
 * <p>
 * Back-pressure (publish delay avg > {@link #THRESHOLD_NS}) indicates producers were
 * blocking on a full send buffer — single-partition saturation at the broker, not
 * overhead from the component under test.
 */
public class BackPressureAnalyser {

    /** 1ms in nanoseconds — delay above this indicates producer back-pressure. */
    public static final double THRESHOLD_NS = 1_000_000.0;

    public record LabelledResult(String label, OmbResult result) {}

    public record Report(String label, double delayAvgNs, double delayP99Ns, boolean saturated,
                         long suggestedMaxRate, long suggestedMinRate) {}

    public List<Report> analyse(List<LabelledResult> results) {
        return results.stream()
                .filter(lr -> lr.result().getPublishDelayLatencyAvgNs() > THRESHOLD_NS)
                .map(lr -> {
                    OmbResult r = lr.result();
                    long maxRate = Math.round(r.getPublishRate() / 1000.0) * 1000L;
                    long minRate = Math.round(maxRate * 0.2);
                    return new Report(lr.label(), r.getPublishDelayLatencyAvgNs(),
                            r.getPublishDelayLatency99pctNs(), true, maxRate, minRate);
                })
                .toList();
    }
}
