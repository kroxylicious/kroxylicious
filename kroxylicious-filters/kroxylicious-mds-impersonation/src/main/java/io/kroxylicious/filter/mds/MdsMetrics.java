/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/** Fixed-cardinality measurements; no principal, token, session ID or endpoint tags. */
final class MdsMetrics {
    private MdsMetrics() {
    }

    static void token(String virtualCluster, String outcome, long started) {
        Timer.builder("kroxylicious.mds.token.duration")
                .description("MDS token acquisition latency, including unsuccessful requests.")
                .tag("virtual_cluster", virtualCluster).tag("outcome", outcome)
                .register(Metrics.globalRegistry).record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
    }

    static void rejected(String virtualCluster, MdsFailure.Reason reason) {
        Counter.builder("kroxylicious.mds.requests.rejected")
                .description("MDS requests refused locally by shared admission control.")
                .tag("virtual_cluster", virtualCluster).tag("reason", label(reason))
                .register(Metrics.globalRegistry).increment();
    }

    static void authentication(String virtualCluster, boolean renewal, String outcome) {
        Counter.builder("kroxylicious.mds.authentication")
                .description("Completed upstream authentication attempts, including renewal.")
                .tag("virtual_cluster", virtualCluster).tag("phase", renewal ? "renewal" : "initial")
                .tag("outcome", outcome).register(Metrics.globalRegistry).increment();
    }

    static void closed(String virtualCluster, MdsFailure.Reason reason) {
        Counter.builder("kroxylicious.mds.connections.closed")
                .description("Connections closed by MDS authentication policy, counted once per connection.")
                .tag("virtual_cluster", virtualCluster).tag("reason", label(reason))
                .register(Metrics.globalRegistry).increment();
    }

    static String label(MdsFailure.Reason reason) {
        return reason.name().toLowerCase(Locale.ROOT);
    }
}
