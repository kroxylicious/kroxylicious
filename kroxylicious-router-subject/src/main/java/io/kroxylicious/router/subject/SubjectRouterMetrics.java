/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

/**
 * Factory methods for the meters published by {@link SubjectRouter}.
 */
final class SubjectRouterMetrics {

    /** The name of the metric label holding the virtual cluster name. */
    static final String VIRTUAL_CLUSTER_LABEL = "virtual_cluster";
    /** The name of the metric label holding the router name. */
    static final String ROUTER_LABEL = "router";
    /** The name of the metric label holding the bounded rejection reason. */
    static final String REASON_LABEL = "reason";

    private static final String REJECTED_TOTAL = "kroxylicious_subject_router_rejected_total";

    private SubjectRouterMetrics() {
    }

    /**
     * Creates a provider of counters of requests rejected fail-closed by the router, keyed by
     * {@link #REASON_LABEL} at increment time.
     *
     * @param virtualCluster the virtual cluster name
     * @param router the router name
     * @return a provider of rejection counters
     */
    @NonNull
    static Meter.MeterProvider<Counter> rejectedCounter(String virtualCluster, String router) {
        return Counter.builder(REJECTED_TOTAL)
                .description("A count of requests rejected fail-closed by the subject router.")
                .tag(VIRTUAL_CLUSTER_LABEL, virtualCluster)
                .tag(ROUTER_LABEL, router)
                .withRegistry(globalRegistry);
    }
}
