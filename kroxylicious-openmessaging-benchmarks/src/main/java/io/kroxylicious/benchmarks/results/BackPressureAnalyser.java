/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.time.Duration;
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
    public static final long THRESHOLD_NS = Duration.ofMillis(1).toNanos();

    public record LabelledResult(String label, OmbResult result) {}

    public record Report(String label, double delayAvgNs, double delayP99Ns, boolean saturated,
                         long suggestedMaxRate, long suggestedMinRate) {}

    public List<Report> analyse(List<LabelledResult> results) {
        return results.stream()
                .filter(lr -> lr.result().getPublishDelayLatencyAvgNs() > THRESHOLD_NS)
                .map(lr -> {
                    OmbResult r = lr.result();
                    // Round to the nearest 1000 msg/sec for clean sweep boundaries.
                    long maxRate = Math.max(1000L, Math.round(r.getPublishRate() / 1000.0) * 1000L);
                    long minRate = maxRate / 2;
                    return new Report(lr.label(), r.getPublishDelayLatencyAvgNs(),
                            r.getPublishDelayLatency99pctNs(), true, maxRate, minRate);
                })
                .toList();
    }
}
